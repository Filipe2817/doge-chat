package com.doge.client.handler;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.common.Handler;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.MessageWrapper;

public class ChatMessageHandler implements Handler {
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
