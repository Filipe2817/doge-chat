package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.PubEndpoint;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final PubEndpoint chatServerPubEndpoint;
    private final Logger logger;

    public ChatMessageHandler(PubEndpoint chatServerPubEndpoint, Logger logger) {
        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ChatMessage chatMessage = wrapper.getChatMessage();
        String topic = chatMessage.getTopic();

        logger.info("Received message from " + chatMessage.getClientId() + ": " + chatMessage.getContent() + " on topic " + topic);

        MessageWrapper forwardWrapper = createForwardMessageWrapper(chatMessage);
        chatServerPubEndpoint.send(topic, forwardWrapper);

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
