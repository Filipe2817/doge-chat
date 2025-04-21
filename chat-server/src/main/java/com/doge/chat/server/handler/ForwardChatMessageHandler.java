package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.PubEndpoint;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ForwardChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final PubEndpoint clientPubEndpoint;
    private final Logger logger;

    public ForwardChatMessageHandler(PubEndpoint clientPubEndpoint, Logger logger) {
        this.clientPubEndpoint = clientPubEndpoint;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ForwardChatMessage forwardChatMessage = wrapper.getForwardChatMessage();
        ChatMessage chatMessage = forwardChatMessage.getChatMessage();
        String topic = chatMessage.getTopic();

        logger.info("Received forwarded message from " + chatMessage.getClientId() + ": " + chatMessage.getContent() + " on topic " + topic);

        MessageWrapper messageWrapper = createMessageWrapper(chatMessage);
        clientPubEndpoint.send(topic, messageWrapper);

        logger.info("Sent message to topic " + topic);
    }

    private MessageWrapper createMessageWrapper(ChatMessage chatMessage) {
        return MessageWrapper.newBuilder()
                .setChatMessage(chatMessage)
                .build();
    }
}
