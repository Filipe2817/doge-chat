package com.doge.client.command;

import java.util.Comparator;

import com.doge.client.Console;

public class HelpCommand extends AbstractCommand {
    private final CommandManager commandManager;

    public HelpCommand(CommandManager commandManager) {
        super("/help");
        this.commandManager = commandManager;
    }

    @Override
    public void execute(Console console, String[] args) {
        console.info("Available commands:");

        commandManager.getCommands()
            .stream()
            .sorted(Comparator.comparing(Command::getName))
            .forEach(command -> console.info(" - " + command.getUsage()));
    }
}
