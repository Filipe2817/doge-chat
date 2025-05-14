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

        String topic = client.getCurrentTopic();
        String clientId = client.getId();

        MessageWrapper exitMessage = createExitMessage(topic, clientId);
        this.pushEndpoint.send(exitMessage);

        this.client.stop();
    }

    private MessageWrapper createExitMessage(String topic, String clientId) {
        ExitMessage exitMessage = ExitMessage.newBuilder()
                .setTopic(topic)
                .setClientId(clientId)
                .build();

        return MessageWrapper.newBuilder()
                .setExitMessage(exitMessage)
                .build();
    }
}
