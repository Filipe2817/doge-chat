package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.PusherSocketWrapper;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.MessageWrapper;

public class SendMessageCommand extends AbstractCommand {
    private final Client client;
    private final PusherSocketWrapper pusherWrapper;

    public SendMessageCommand(Client client, PusherSocketWrapper socketWrapper) {
        super("/send", "<message>");
        this.client = client;
        this.pusherWrapper = socketWrapper;
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

        MessageWrapper wrapper = createChatMessageWrapper(message, clientId, topic);
        this.pusherWrapper.sendMessage(topic, wrapper);
        console.info("Sent message: " + message + " to topic: " + topic);
    }

    private MessageWrapper createChatMessageWrapper(String message, String clientId, String topic) {
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
