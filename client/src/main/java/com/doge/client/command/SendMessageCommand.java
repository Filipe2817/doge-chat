package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.zmq.PushEndpoint;

public class SendMessageCommand extends AbstractCommand {
    private final Client client;

    private final PushEndpoint pushEndpoint;

    public SendMessageCommand(Client client, PushEndpoint pushEndpoint) {
        super("/send", "<message>");
        this.client = client;

        this.pushEndpoint = pushEndpoint;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length < 1) {
            sendUsage(console);
            return;
        }

        String message = String.join(" ", args);
        String clientId = client.getId();
        String topic = client.getCurrentTopic();

        // FIXME: Why is the topic needed here? Can't it be inside the message?
        MessageWrapper wrapper = createChatMessage(message, clientId, topic);
        this.pushEndpoint.send(topic, wrapper);
        
        console.info("Sent message to topic '" + topic + "'");
    }

    private MessageWrapper createChatMessage(String message, String clientId, String topic) {
        ChatMessage chatMessage = ChatMessage.newBuilder()
                .setClientId(client.getId())
                .setTopic(topic)
                .setContent(message)
                .build();

        return MessageWrapper.newBuilder()
                .setChatMessage(chatMessage)
                .build();
    }
}
