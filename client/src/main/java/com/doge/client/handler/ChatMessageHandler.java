package com.doge.client.handler;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final Client client;
    private final Console console;

    public ChatMessageHandler(Client client, Console console) {
        this.client = client;
        this.console = console;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ChatMessage chatMessage = wrapper.getChatMessage();
        String clientId = chatMessage.getClientId();
        String content = chatMessage.getContent();

        if (clientId.equals(client.getId())) {
            console.info("[You] " + content);
        } else {
            console.info("[" + clientId + "] " + content);
        }
    }
}
