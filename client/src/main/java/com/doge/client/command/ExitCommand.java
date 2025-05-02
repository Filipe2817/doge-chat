package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.zmq.PushEndpoint;
import com.doge.common.proto.ExitMessage;
import com.doge.common.proto.MessageWrapper;

public class ExitCommand extends AbstractCommand {
    private final Client client;

    private final PushEndpoint pushEndpoint;

    public ExitCommand(Client client, PushEndpoint pushEndpoint) {
        super("/exit");
        this.client = client;

        this.pushEndpoint = pushEndpoint;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length > 0) {
            sendUsage(console);
            return;
        }

        MessageWrapper exitMessage = createExitMessage();
        this.pushEndpoint.send(exitMessage);

        this.client.stop();
    }

    private MessageWrapper createExitMessage() {
        ExitMessage exitMessage = ExitMessage.newBuilder()
                .setClientId(client.getId())
                .build();

        return MessageWrapper.newBuilder()
                .setExitMessage(exitMessage)
                .build();
    }
}
