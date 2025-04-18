package com.doge.client.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.doge.client.Console;

public class CommandManager {
    private final Map<String, Command> commands;
    
    public CommandManager() {
        this.commands = new HashMap<>();
    }

    public void registerCommand(Command command) {
        this.commands.put(command.getName().toLowerCase(), command);
    }

    public Command getCommand(String name) {
        return this.commands.get(name.toLowerCase());
    }

    public Collection<Command> getCommands() {
        return this.commands.values();
    }

    public void handleCommand(Console console, String rawCommand) {
        String[] split = rawCommand.trim().split(" ");
        if (split.length == 0) {
            return;
        }

        String commandName = split[0];
        String[] args = new String[split.length - 1];
        System.arraycopy(split, 1, args, 0, split.length - 1);

        Command command = this.getCommand(commandName);
        if (command == null) {
            console.error(commandName + " is not a valid command. Type /help for a list of commands.");
            return;
        }

        command.execute(console, args);
    }
}
