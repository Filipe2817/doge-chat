package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.PublisherSocketWrapper;
import com.doge.common.Handler;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;

public class ChatMessageHandler implements Handler {
    private final PublisherSocketWrapper chatServerPublisherWrapper;
    private final Logger logger;

    public ChatMessageHandler(PublisherSocketWrapper chatServerPublisherWrapper, Logger logger) {
        this.chatServerPublisherWrapper = chatServerPublisherWrapper;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ChatMessage chatMessage = wrapper.getChatMessage();
        String topic = chatMessage.getTopic();

        logger.info("Received message from " + chatMessage.getClientId() + ": " + chatMessage.getContent() + " on topic " + topic);

        MessageWrapper forwardWrapper = createForwardMessageWrapper(chatMessage);
        chatServerPublisherWrapper.sendMessage(topic, forwardWrapper);

        logger.info("Forwarded message to topic " + topic);
    }

    private MessageWrapper createForwardMessageWrapper(ChatMessage chatMessage) {
        ForwardChatMessage forwardChatMessage = ForwardChatMessage.newBuilder()
                .setChatMessage(chatMessage)
                .build();

        return MessageWrapper.newBuilder()
                .setForwardChatMessage(forwardChatMessage)
                .build();
    }
}
