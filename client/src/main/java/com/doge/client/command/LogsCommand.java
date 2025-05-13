package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.reactive.ReactiveGrpcClient;

public class LogsCommand extends AbstractCommand {
    private final Client client;

    private final ReactiveGrpcClient reactiveClient;

    public LogsCommand(Client client, ReactiveGrpcClient reactiveClient) {
        super("/logs", "<last> <userId?>");
        this.client = client;

        this.reactiveClient = reactiveClient;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length < 1 || args.length > 2) {
            sendUsage(console);
            return;
        }

        int last = 0;
        String userId = null;

        try {
            last = Integer.parseInt(args[0]);
            if (args.length == 2) {
                userId = args[1];
            }
        } catch (NumberFormatException e) {
            console.error("Invalid number given to argument 'last'");
            sendUsage(console);
            return;
        }

        if (last < 1) {
            console.error("Argument 'last' must be greater than 0");
            sendUsage(console);
            return;
        }

        if (userId != null) {
            reactiveClient.getUserLogs(client.getCurrentTopic(), userId, last);
        } else {
            reactiveClient.getLogs(client.getCurrentTopic(), last);
        }
    }
}
