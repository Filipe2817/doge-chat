package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;

public class ExitCommand extends AbstractCommand {
    private final Client client;

    public ExitCommand(Client client) {
        super("/exit");
        this.client = client;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length > 0) {
            sendUsage(console);
            return;
        }

        this.client.stop();
    }
}
