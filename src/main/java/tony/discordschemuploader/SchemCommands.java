package tony.discordschemuploader;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Discord slash commands for WorldEdit schematics:
 * <ul>
 *     <li>{@code /upload file [force]} saves the attached .schem file into WorldEdit's schematics
 *     folder under its original filename; {@code force} overwrites an existing one.</li>
 *     <li>{@code /download name} posts a schematic from that folder, with autocomplete on names.</li>
 *     <li>{@code /vcsdownload build [version]} posts a version of an MCVCS build, the latest one by default,
 *     with autocomplete on builds and on the chosen build's versions.</li>
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
	private static final String VCS_DOWNLOAD = "vcsdownload";
	private static final String FILE_OPTION = "file";
	private static final String NAME_OPTION = "name";
	private static final String FORCE_OPTION = "force";
	private static final String BUILD_OPTION = "build";
	private static final String VERSION_OPTION = "version";
	private static final String EXTENSION = ".schem";

	/** MCVCS's {@code Build.NAME}; also keeps build names from escaping the MCVCS folder. */
	private static final Pattern MCVCS_BUILD_NAME = Pattern.compile("[A-Za-z0-9_+][A-Za-z0-9_+-]*(\\.[A-Za-z0-9_+-]+)*");

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
				case VCS_DOWNLOAD -> handleVcsDownload(event);
				default -> Mono.empty();
			};
			return handler.onErrorResume(e -> {
				DiscordSchemUploader.LOGGER.error("Failed to handle /{}", event.getCommandName(), e);
				return Mono.empty();
			});
		}).subscribe();

		client.on(ChatInputAutoCompleteEvent.class, event -> {
			if (!isAllowedChannel(event.getInteraction())) {
				return event.respondWithSuggestions(List.of());
			}
			Mono<List<ApplicationCommandOptionChoiceData>> suggestions = switch (event.getCommandName()) {
				case DOWNLOAD -> suggestSchematics(typed(event));
				case VCS_DOWNLOAD -> BUILD_OPTION.equals(event.getFocusedOption().getName())
						? suggestBuilds(typed(event))
						: suggestVersions(optionString(event.getOption(BUILD_OPTION)), typed(event));
				default -> Mono.empty();
			};
			return suggestions.flatMap(event::respondWithSuggestions)
					.onErrorResume(e -> {
						DiscordSchemUploader.LOGGER.error("Failed to autocomplete /{}", event.getCommandName(), e);
						return Mono.empty();
					});
		}).subscribe();

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
						.addOption(ApplicationCommandOptionData.builder()
								.name(FORCE_OPTION)
								.description("Overwrite an existing schematic with the same name")
								.type(ApplicationCommandOption.Type.BOOLEAN.getValue())
								.required(false)
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
						.build(),
				ApplicationCommandRequest.builder()
						.name(VCS_DOWNLOAD)
						.description("Download a version of an MCVCS build")
						.addOption(ApplicationCommandOptionData.builder()
								.name(BUILD_OPTION)
								.description("The build's name")
								.type(ApplicationCommandOption.Type.STRING.getValue())
								.required(true)
								.autocomplete(true)
								.build())
						.addOption(ApplicationCommandOptionData.builder()
								.name(VERSION_OPTION)
								.description("The version number; the latest one if left out")
								.type(ApplicationCommandOption.Type.INTEGER.getValue())
								.required(false)
								.autocomplete(true)
								.minValue(1.0)
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
		boolean force = event.getOption(FORCE_OPTION)
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asBoolean)
				.orElse(false);
		boolean existed = Files.exists(target);
		if (existed && !force) {
			return refuse(event, alreadyExists(filename));
		}

		return event.deferReply()
				.then(fetch(attachment.getUrl()))
				.flatMap(bytes -> Mono.fromCallable(() -> save(dir, target, bytes, force)))
				.flatMap(saved -> event.editReply(!saved
						? alreadyExists(filename)
						: existed ? "Replaced `" + filename + "`." : "Uploaded `" + filename + "`.").then())
				.onErrorResume(e -> {
					DiscordSchemUploader.LOGGER.error("Failed to upload schematic {}", filename, e);
					return event.editReply("Failed to upload `" + filename + "`.").then();
				});
	}

	private static Mono<Void> handleDownload(ChatInputInteractionEvent event) {
		if (!isAllowedChannel(event.getInteraction())) {
			return refuse(event, "Schematics can't be downloaded in this channel.");
		}

		String name = optionString(event.getOption(NAME_OPTION));
		// Accept "house" as well as "house.schem" when the user didn't pick a suggestion.
		String filename = isSchematicName(name) ? name : name + EXTENSION;

		Path target = resolveInDir(schematicsDir(), filename);
		if (target == null || !Files.isRegularFile(target)) {
			return refuse(event, "No schematic named `" + filename + "`.");
		}
		return sendFile(event, target);
	}

	private static Mono<Void> handleVcsDownload(ChatInputInteractionEvent event) {
		if (!isAllowedChannel(event.getInteraction())) {
			return refuse(event, "Builds can't be downloaded in this channel.");
		}

		String name = optionString(event.getOption(BUILD_OPTION));
		McvcsBuild build = readMcvcsBuild(name).orElse(null);
		if (build == null) {
			return refuse(event, "No MCVCS build named `" + name + "`.");
		}

		long version = event.getOption(VERSION_OPTION)
				.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::asLong)
				.orElse((long) build.latest());
		if (!build.versions().contains(version)) {
			return refuse(event, "Build `" + name + "` has no version " + version + "; the latest is " + build.latest() + ".");
		}
		return sendFile(event, build.schematic(version));
	}

	private static Mono<Void> sendFile(ChatInputInteractionEvent event, Path file) {
		String filename = file.getFileName().toString();
		return event.deferReply()
				.then(Mono.fromCallable(() -> Files.readAllBytes(file)))
				.flatMap(bytes -> event.editReply()
						.withFiles(MessageCreateFields.File.of(filename, new ByteArrayInputStream(bytes)))
						.then())
				.onErrorResume(e -> {
					DiscordSchemUploader.LOGGER.error("Failed to send {}", file, e);
					return event.editReply("Failed to send `" + filename + "`.").then();
				});
	}

	/** Schematic filenames containing what the user has typed so far. */
	private static Mono<List<ApplicationCommandOptionChoiceData>> suggestSchematics(String typed) {
		return Mono.fromCallable(() -> choices(listSchematics().stream()
				.filter(name -> name.toLowerCase(Locale.ROOT).contains(typed))
				.map(name -> new Choice(name, name))));
	}

	/** MCVCS build names containing what the user has typed so far. */
	private static Mono<List<ApplicationCommandOptionChoiceData>> suggestBuilds(String typed) {
		return Mono.fromCallable(() -> choices(listMcvcsBuilds().stream()
				.filter(build -> build.name().toLowerCase(Locale.ROOT).contains(typed))
				.map(build -> new Choice(build.name() + " (latest v" + build.latest() + ")", build.name()))));
	}

	/** Versions of the build chosen so far, newest first, whose number starts with what the user has typed. */
	private static Mono<List<ApplicationCommandOptionChoiceData>> suggestVersions(String buildName, String typed) {
		return Mono.fromCallable(() -> readMcvcsBuild(buildName)
				.map(build -> choices(build.versions().stream()
						.sorted(Comparator.reverseOrder())
						.filter(version -> version.toString().startsWith(typed))
						.map(version -> new Choice(
								"v" + version + (version == build.latest() ? " (latest)" : ""), version))))
				.orElse(List.of()));
	}

	/** The first {@link #MAX_SUGGESTIONS} of {@code choices} that fit Discord's limits. */
	private static List<ApplicationCommandOptionChoiceData> choices(Stream<Choice> choices) {
		return choices.filter(choice -> choice.label().length() <= MAX_CHOICE_LENGTH
						&& choice.value().toString().length() <= MAX_CHOICE_LENGTH)
				.limit(MAX_SUGGESTIONS)
				.map(choice -> (ApplicationCommandOptionChoiceData) ApplicationCommandOptionChoiceData.builder()
						.name(choice.label())
						.value(choice.value())
						.build())
				.toList();
	}

	/** One autocomplete suggestion: what the user sees and what the command receives. */
	private record Choice(String label, Object value) {
	}

	/** What the user has typed so far into the option being autocompleted, lower-cased. */
	private static String typed(ChatInputAutoCompleteEvent event) {
		return event.getFocusedOption().getValue()
				.map(ApplicationCommandInteractionOptionValue::getRaw)
				.orElse("")
				.strip()
				.toLowerCase(Locale.ROOT);
	}

	private static String optionString(Optional<ApplicationCommandInteractionOption> option) {
		return option.flatMap(ApplicationCommandInteractionOption::getValue)
				.map(ApplicationCommandInteractionOptionValue::getRaw)
				.map(String::strip)
				.orElse("");
	}

	/**
	 * An MCVCS build: its latest version and every version whose schematic is on disk. MCVCS keeps each build in
	 * {@code mcvcs/<name>/}: a {@code build.json} whose {@code version} is the latest version and whose
	 * {@code versions} has a key per version, and one {@code <name>-v<N>.schem} per version.
	 */
	private record McvcsBuild(String name, int latest, Set<Long> versions, Path folder) {
		Path schematic(long version) {
			return folder.resolve(name + "-v" + version + EXTENSION);
		}
	}

	/** Every MCVCS build whose latest version is on disk, from every world, sorted by name. */
	private static List<McvcsBuild> listMcvcsBuilds() throws IOException {
		Path root = mcvcsDir();
		if (!Files.isDirectory(root)) {
			return List.of();
		}
		try (Stream<Path> folders = Files.list(root)) {
			return folders.filter(Files::isDirectory)
					.map(folder -> readMcvcsBuild(folder.getFileName().toString()))
					.flatMap(Optional::stream)
					.sorted(Comparator.comparing(McvcsBuild::name, String.CASE_INSENSITIVE_ORDER))
					.toList();
		}
	}

	/** The MCVCS build called {@code name}, if it exists and its latest version is on disk. */
	private static Optional<McvcsBuild> readMcvcsBuild(String name) {
		if (!MCVCS_BUILD_NAME.matcher(name).matches()) {
			return Optional.empty();
		}
		Path folder = mcvcsDir().resolve(name);
		Path buildFile = folder.resolve("build.json");
		if (!Files.isRegularFile(buildFile)) {
			return Optional.empty();
		}
		try {
			JsonObject json = JsonParser.parseString(Files.readString(buildFile)).getAsJsonObject();
			int latest = json.get("version").getAsInt();
			McvcsBuild build = new McvcsBuild(name, latest, new TreeSet<>(), folder);
			for (String key : json.getAsJsonObject("versions").keySet()) {
				long version = Long.parseLong(key);
				if (Files.isRegularFile(build.schematic(version))) {
					build.versions().add(version);
				}
			}
			return build.versions().contains((long) latest) ? Optional.of(build) : Optional.empty();
		} catch (IOException | RuntimeException e) {
			DiscordSchemUploader.LOGGER.warn("Skipping unreadable MCVCS build {}", buildFile, e);
			return Optional.empty();
		}
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

	private static String alreadyExists(String filename) {
		return "A schematic named `" + filename + "` already exists. Use `" + FORCE_OPTION + ": True` to overwrite it.";
	}

	/**
	 * Writes the file. With {@code force} an existing file is replaced atomically, so a failed write
	 * never leaves a truncated schematic behind; otherwise returns false if a file with that name
	 * appeared in the meantime.
	 */
	private static boolean save(Path dir, Path target, byte[] bytes, boolean force) throws IOException {
		Files.createDirectories(dir);
		if (force) {
			Path temp = Files.createTempFile(dir, ".upload-", ".tmp");
			try {
				Files.write(temp, bytes);
				Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			} finally {
				Files.deleteIfExists(temp);
			}
			DiscordSchemUploader.LOGGER.info("Saved uploaded schematic {} (overwrite)", target);
			return true;
		}
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

	/** MCVCS keeps its builds in {@code mcvcs/} in the game directory. */
	private static Path mcvcsDir() {
		return FabricLoader.getInstance().getGameDir().resolve("mcvcs").toAbsolutePath().normalize();
	}

	/** WorldEdit for Fabric keeps schematics in {@code config/worldedit/schematics}. */
	private static Path schematicsDir() {
		return FabricLoader.getInstance().getConfigDir().resolve("worldedit").resolve("schematics").toAbsolutePath().normalize();
	}
}
