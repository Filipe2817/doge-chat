package com.doge.client.command;

import com.doge.client.Console;

public abstract class AbstractCommand implements Command {
    private final String name;
    private final String usage;

    public AbstractCommand(String name, String usage) {
        this.name = name;
        this.usage = usage;
    }

    public AbstractCommand(String name) {
        this(name, "");
    }

    public void sendUsage(Console console) {
        console.warn("Usage: " + getUsage());
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getUsage() {
        return this.name + " " + this.usage;
    }

    @Override
    public abstract void execute(Console console, String[] args);
}
