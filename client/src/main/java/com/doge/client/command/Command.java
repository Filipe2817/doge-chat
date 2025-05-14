package com.doge.client.command;

import com.doge.client.Console;

public interface Command {
    String getName();
    
    String getUsage();

    void execute(Console console, String[] args);
}
