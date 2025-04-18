package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.PublisherSocketWrapper;
import com.doge.common.Handler;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;

public class ForwardChatMessageHandler implements Handler {
    private final PublisherSocketWrapper clientPublisherWrapper;
    private final Logger logger;

    public ForwardChatMessageHandler(PublisherSocketWrapper clientPublisherWrapper, Logger logger) {
        this.clientPublisherWrapper = clientPublisherWrapper;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ForwardChatMessage forwardChatMessage = wrapper.getForwardChatMessage();
        ChatMessage chatMessage = forwardChatMessage.getChatMessage();
        String topic = chatMessage.getTopic();

        logger.info("Received forwarded message from " + chatMessage.getClientId() + ": " + chatMessage.getContent() + " on topic " + topic);

        MessageWrapper messageWrapper = createMessageWrapper(chatMessage);
        clientPublisherWrapper.sendMessage(topic, messageWrapper);

        logger.info("Sent message to topic " + topic);
    }

    private MessageWrapper createMessageWrapper(ChatMessage chatMessage) {
        return MessageWrapper.newBuilder()
                .setChatMessage(chatMessage)
                .build();
    }
}
