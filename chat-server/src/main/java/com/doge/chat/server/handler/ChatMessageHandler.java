package com.doge.chat.server.handler;

import java.util.Map;

import com.doge.chat.server.ChatServer;
import com.doge.chat.server.Logger;
import com.doge.chat.server.causal.VectorClock;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final ChatServer chatServer;
    private final Logger logger;

    private final PubEndpoint clientPubEndpoint;
    private final PubEndpoint chatServerPubEndpoint;
    private final VectorClockManager vectorClockManager;

    public ChatMessageHandler(
        ChatServer chatServer,
        Logger logger,
        PubEndpoint clientPubEndpoint, 
        PubEndpoint chatServerPubEndpoint, 
        VectorClockManager vectorClockManager
    ) {
        this.chatServer = chatServer;
        this.logger = logger;

        this.clientPubEndpoint = clientPubEndpoint;
        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.vectorClockManager = vectorClockManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ChatMessage chatMessage = wrapper.getChatMessage();
        String topic = chatMessage.getTopic();
        String clientId = chatMessage.getClientId();
        String content = chatMessage.getContent();

        logger.info("Received message from '" + clientId + "' on topic '" + topic + "' with content: " + content);
        this.clientPubEndpoint.send(topic, wrapper);

        this.vectorClockManager.selfIncrementForTopic(topic);
        logger.debug("Incremented self vector clock for topic " + "'" + topic + "'");

        VectorClock vectorClock = this.vectorClockManager.getByTopic(topic);
        MessageWrapper forward = createForwardMessage(chatMessage, vectorClock.asData());
        this.chatServerPubEndpoint.send(topic, forward);

        logger.info("Forwarded message to topic '" + topic + "' with content: " + content);
    }

    private MessageWrapper createForwardMessage(ChatMessage chatMessage, Map<Integer, Integer> vectorClockData) {
        ForwardChatMessage forwardChatMessage = ForwardChatMessage.newBuilder()
                .setChatMessage(chatMessage)
                .setSenderId(chatServer.getId())
                .putAllVectorClock(vectorClockData)
                .build();

        return MessageWrapper.newBuilder()
                .setForwardChatMessage(forwardChatMessage)
                .build();
    }
}
