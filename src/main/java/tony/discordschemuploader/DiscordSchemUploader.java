package tony.discordschemuploader;

import fr.denisd3d.mc2discord.core.Mc2Discord;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.Identifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscordSchemUploader implements ModInitializer {
	public static final String MOD_ID = "discord-schem-uploader";

	// This logger is used to write text to the console and the log file.
	// It is considered best practice to use your mod id as the logger's name.
	// That way, it's clear which mod wrote info, warnings, and errors.
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

	@Override
	public void onInitialize() {
		ServerTickEvents.END_SERVER_TICK.register(server -> SchemCommands.tick());

		CommandRegistrationCallback.EVENT.register((dispatcher, registryAccess, environment) ->
				dispatcher.register(Commands.literal("schemdiscord")
						.then(Commands.literal("status").executes(ctx -> {
							ctx.getSource().sendSuccess(() -> Component.literal(discordStatus()), false);
							return 1;
						}))));
	}

	/** Reports whether Mc2Discord's bot is logged in, for verifying the test connection. */
	private static String discordStatus() {
		Mc2Discord m2d = Mc2Discord.INSTANCE;
		if (m2d == null) {
			return "Mc2Discord is not initialized (server not started yet?)";
		}
		if (!m2d.errors.isEmpty()) {
			return "Mc2Discord errors: " + String.join("; ", m2d.errors);
		}
		if (m2d.client == null) {
			return "Mc2Discord bot is not connected yet";
		}
		return "Mc2Discord bot connected, id " + m2d.client.getSelfId().asString();
	}

	public static Identifier id(String path) {
		return Identifier.fromNamespaceAndPath(MOD_ID, path);
	}
}
