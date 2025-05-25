package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.reactive.ReactiveGrpcClient;

public class CancelCommand extends AbstractCommand {
    private Client client;

    private ReactiveGrpcClient reactiveClient;

    public CancelCommand(Client client, ReactiveGrpcClient reactiveClient) {
        super("/cancel");
        this.client = client;

        this.reactiveClient = reactiveClient;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length != 0) {
            sendUsage(console);
            return;
        }

        if (this.client.isRpcOngoing()) {
            this.reactiveClient.cancel();
            this.client.rpcDone();

            console.info("Ongoing RPC has been cancelled successfully");
        } else {
            console.error("No ongoing RPC to cancel");
        }
    }
}
