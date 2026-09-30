package tony.discordschemuploader;

import fr.denisd3d.mc2discord.core.Mc2Discord;
import fr.denisd3d.mc2discord.shadow.discord4j.common.util.Snowflake;
import fr.denisd3d.mc2discord.shadow.discord4j.core.GatewayDiscordClient;
import fr.denisd3d.mc2discord.shadow.discord4j.core.event.domain.interaction.ChatInputAutoCompleteEvent;
import fr.denisd3d.mc2discord.shadow.discord4j.core.event.domain.interaction.ChatInputInteractionEvent;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.command.ApplicationCommandInteractionOption;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.command.ApplicationCommandInteractionOptionValue;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.command.ApplicationCommandOption;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.command.Interaction;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.entity.Attachment;
import fr.denisd3d.mc2discord.shadow.discord4j.core.object.entity.channel.GuildChannel;
import fr.denisd3d.mc2discord.shadow.discord4j.core.spec.MessageCreateFields;
import fr.denisd3d.mc2discord.shadow.discord4j.discordjson.json.ApplicationCommandOptionChoiceData;
import fr.denisd3d.mc2discord.shadow.discord4j.discordjson.json.ApplicationCommandOptionData;
import fr.denisd3d.mc2discord.shadow.discord4j.discordjson.json.ApplicationCommandRequest;
import fr.denisd3d.mc2discord.shadow.reactor.core.publisher.Flux;
import fr.denisd3d.mc2discord.shadow.reactor.core.publisher.Mono;
import net.fabricmc.loader.api.FabricLoader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Discord slash commands for WorldEdit schematics:
 * <ul>
 *     <li>{@code /upload file} saves the attached .schem file into WorldEdit's schematics folder
 *     under its original filename.</li>
 *     <li>{@code /download name} posts a schematic from that folder, with autocomplete on names.</li>
 * </ul>
 *
 * <p>Piggybacks on Mc2Discord's bot. Commands are accepted only in channels listed in
 * Mc2Discord's config. Slash commands are live interactions, so nothing posted while the
 * server was offline is replayed on startup, and deleting a Discord message never touches
 * saved files.
 */
public final class SchemCommands {
	private static final String UPLOAD = "upload";
	private static final String DOWNLOAD = "download";
	private static final String FILE_OPTION = "file";
	private static final String NAME_OPTION = "name";
	private static final String EXTENSION = ".schem";

	/** Discord caps autocomplete at 25 choices of at most 100 characters each. */
	private static final int MAX_SUGGESTIONS = 25;
	private static final int MAX_CHOICE_LENGTH = 100;

	private static final HttpClient HTTP = HttpClient.newBuilder()
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();

	/** Client we last attached to; Mc2Discord replaces its client on {@code /mc2discord restart}. */
	private static GatewayDiscordClient attachedClient;

	private SchemCommands() {
	}

	/** Called every server tick; attaches to Mc2Discord's bot once it has logged in (or re-logged in). */
	public static void tick() {
		Mc2Discord m2d = Mc2Discord.INSTANCE;
		if (m2d == null || m2d.client == null || m2d.client == attachedClient) {
			return;
		}
		attachedClient = m2d.client;
		attach(m2d);
	}

	private static void attach(Mc2Discord m2d) {
		GatewayDiscordClient client = m2d.client;

		client.on(ChatInputInteractionEvent.class, event -> {
			Mono<Void> handler = switch (event.getCommandName()) {
				case UPLOAD -> handleUpload(event);
				case DOWNLOAD -> handleDownload(event);
				default -> Mono.empty();
			};
			return handler.onErrorResume(e -> {
				DiscordSchemUploader.LOGGER.error("Failed to handle /{}", event.getCommandName(), e);
				return Mono.empty();
			});
		}).subscribe();

		client.on(ChatInputAutoCompleteEvent.class, event -> DOWNLOAD.equals(event.getCommandName())
						? suggestSchematics(event).onErrorResume(e -> {
							DiscordSchemUploader.LOGGER.error("Failed to autocomplete /{}", DOWNLOAD, e);
							return Mono.empty();
						})
						: Mono.empty())
				.subscribe();

		List<ApplicationCommandRequest> requests = List.of(
				ApplicationCommandRequest.builder()
						.name(UPLOAD)
						.description("Upload a .schem file to the server's WorldEdit schematics")
						.addOption(ApplicationCommandOptionData.builder()
								.name(FILE_OPTION)
								.description("The .schem file; it is saved under its filename")
								.type(ApplicationCommandOption.Type.ATTACHMENT.getValue())
								.required(true)
								.build())
						.build(),
				ApplicationCommandRequest.builder()
						.name(DOWNLOAD)
						.description("Download a schematic from the server's WorldEdit schematics")
						.addOption(ApplicationCommandOptionData.builder()
								.name(NAME_OPTION)
								.description("The schematic's filename, e.g. house.schem")
								.type(ApplicationCommandOption.Type.STRING.getValue())
								.required(true)
								.autocomplete(true)
								.build())
						.build());

		// Register as guild commands in every guild that has a configured Mc2Discord channel.
		client.getRestClient().getApplicationId()
				.flatMapMany(appId -> Flux.fromIterable(m2d.config.channels.channels)
						.flatMap(channel -> client.getChannelById(channel.channel_id)
								.ofType(GuildChannel.class)
								.onErrorResume(e -> Mono.empty()))
						.map(GuildChannel::getGuildId)
						.distinct()
						.flatMap(guildId -> Flux.fromIterable(requests)
								.flatMap(request -> client.getRestClient().getApplicationService()
										.createGuildApplicationCommand(appId, guildId.asLong(), request)
										.doOnSuccess(data -> DiscordSchemUploader.LOGGER.info(
												"Registered /{} in guild {}", request.name(), guildId.asString())))))
				.subscribe(data -> {
				}, e -> DiscordSchemUploader.LOGGER.error("Failed to register schematic commands", e));
	}

	private static Mono<Void> handleUpload(ChatInputInteractionEvent event) {
		if (!isAllowedChannel(event.getInteraction())) {
			return refuse(event, "Schematics can't be uploaded in this channel.");
		}

		Attachment attachment = event.getOption(FILE_OPTION)
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asAttachment)
				.orElse(null);
		if (attachment == null) {
			return refuse(event, "Attach a " + EXTENSION + " file.");
		}

		String filename = attachment.getFilename();
		if (!isSchematicName(filename)) {
			return refuse(event, "Only " + EXTENSION + " files can be uploaded.");
		}

		Path dir = schematicsDir();
		Path target = resolveInDir(dir, filename);
		if (target == null) {
			return refuse(event, "`" + filename + "` is not a valid schematic filename.");
		}
		if (Files.exists(target)) {
			return refuse(event, "A schematic named `" + filename + "` already exists.");
		}

		return event.deferReply()
				.then(fetch(attachment.getUrl()))
				.flatMap(bytes -> Mono.fromCallable(() -> save(dir, target, bytes)))
				.flatMap(saved -> event.editReply(saved
						? "Uploaded `" + filename + "`."
						: "A schematic named `" + filename + "` already exists.").then())
				.onErrorResume(e -> {
					DiscordSchemUploader.LOGGER.error("Failed to upload schematic {}", filename, e);
					return event.editReply("Failed to upload `" + filename + "`.").then();
				});
	}

	private static Mono<Void> handleDownload(ChatInputInteractionEvent event) {
		if (!isAllowedChannel(event.getInteraction())) {
			return refuse(event, "Schematics can't be downloaded in this channel.");
		}

		String name = event.getOption(NAME_OPTION)
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asString)
				.map(String::strip)
				.orElse("");
		// Accept "house" as well as "house.schem" when the user didn't pick a suggestion.
		String filename = isSchematicName(name) ? name : name + EXTENSION;

		Path target = resolveInDir(schematicsDir(), filename);
		if (target == null || !Files.isRegularFile(target)) {
			return refuse(event, "No schematic named `" + filename + "`.");
		}

		return event.deferReply()
				.then(Mono.fromCallable(() -> Files.readAllBytes(target)))
				.flatMap(bytes -> event.editReply()
						.withFiles(MessageCreateFields.File.of(filename, new ByteArrayInputStream(bytes)))
						.then())
				.onErrorResume(e -> {
					DiscordSchemUploader.LOGGER.error("Failed to send schematic {}", filename, e);
					return event.editReply("Failed to send `" + filename + "`.").then();
				});
	}

	/** Suggests schematic filenames containing what the user has typed so far. */
	private static Mono<Void> suggestSchematics(ChatInputAutoCompleteEvent event) {
		if (!isAllowedChannel(event.getInteraction())) {
			return event.respondWithSuggestions(List.of());
		}

		String typed = event.getFocusedOption().getValue()
				.map(ApplicationCommandInteractionOptionValue::getRaw)
				.orElse("")
				.strip()
				.toLowerCase(Locale.ROOT);

		return Mono.fromCallable(() -> listSchematics().stream()
						.filter(name -> name.toLowerCase(Locale.ROOT).contains(typed))
						.limit(MAX_SUGGESTIONS)
						.map(name -> (ApplicationCommandOptionChoiceData) ApplicationCommandOptionChoiceData.builder()
								.name(name)
								.value(name)
								.build())
						.toList())
				.flatMap(event::respondWithSuggestions);
	}

	/** Filenames of the .schem files directly in the schematics folder, sorted case-insensitively. */
	private static List<String> listSchematics() throws IOException {
		Path dir = schematicsDir();
		if (!Files.isDirectory(dir)) {
			return List.of();
		}
		try (Stream<Path> files = Files.list(dir)) {
			return files.filter(Files::isRegularFile)
					.map(path -> path.getFileName().toString())
					.filter(SchemCommands::isSchematicName)
					.filter(name -> name.length() <= MAX_CHOICE_LENGTH)
					.sorted(String.CASE_INSENSITIVE_ORDER)
					.toList();
		}
	}

	private static boolean isAllowedChannel(Interaction interaction) {
		Mc2Discord m2d = Mc2Discord.INSTANCE;
		Snowflake channelId = interaction.getChannelId();
		return m2d != null && m2d.config.channels.channels.stream()
				.map(channel -> channel.channel_id)
				.anyMatch(channelId::equals);
	}

	private static boolean isSchematicName(String filename) {
		return filename.toLowerCase(Locale.ROOT).endsWith(EXTENSION);
	}

	/** Returns the path of {@code filename} directly inside {@code dir}, or null if the name would escape it. */
	private static Path resolveInDir(Path dir, String filename) {
		try {
			Path target = dir.resolve(filename).normalize();
			return dir.equals(target.getParent()) && target.getFileName().toString().equals(filename) ? target : null;
		} catch (InvalidPathException e) {
			return null;
		}
	}

	private static Mono<byte[]> fetch(String url) {
		HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
		return Mono.fromFuture(() -> HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()))
				.flatMap(response -> response.statusCode() == 200
						? Mono.just(response.body())
						: Mono.error(new IOException("Attachment download returned HTTP " + response.statusCode())));
	}

	/** Writes the file; returns false if a file with that name appeared in the meantime. */
	private static boolean save(Path dir, Path target, byte[] bytes) throws IOException {
		Files.createDirectories(dir);
		try {
			Files.write(target, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
			DiscordSchemUploader.LOGGER.info("Saved uploaded schematic {}", target);
			return true;
		} catch (FileAlreadyExistsException e) {
			return false;
		}
	}

	private static Mono<Void> refuse(ChatInputInteractionEvent event, String message) {
		return event.reply(message).withEphemeral(true).then();
	}

	/** WorldEdit for Fabric keeps schematics in {@code config/worldedit/schematics}. */
	private static Path schematicsDir() {
		return FabricLoader.getInstance().getConfigDir().resolve("worldedit").resolve("schematics").toAbsolutePath().normalize();
	}
}
