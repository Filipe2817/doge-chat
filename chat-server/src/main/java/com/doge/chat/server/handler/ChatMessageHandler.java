package com.doge.chat.server.handler;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.Pair;

import com.doge.chat.server.ChatServer;
import com.doge.chat.server.causal.VectorClock;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.log.LogManager;
import com.doge.common.Logger;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.PubEndpoint;

public class ChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final ChatServer chatServer;
    private final Logger logger;

    private final PubEndpoint clientPubEndpoint;
    private BlockingQueue<Pair<String, MessageWrapper>> chatServerPubQueue;

    private final VectorClockManager vectorClockManager;
    private final LogManager logsManager;

    public ChatMessageHandler(
        ChatServer chatServer,
        Logger logger,
        PubEndpoint clientPubEndpoint,
        BlockingQueue<Pair<String, MessageWrapper>> chatServerPubQueue,
        VectorClockManager vectorClockManager,
        LogManager logsManager
    ) {
        this.chatServer = chatServer;
        this.logger = logger;

        this.clientPubEndpoint = clientPubEndpoint;
        this.chatServerPubQueue = chatServerPubQueue;

        this.vectorClockManager = vectorClockManager;
        this.logsManager = logsManager;
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
        VectorClock currenVectorClock = this.vectorClockManager.getByTopic(topic);
        logger.debug("Incremented self vector clock for topic " + "'" + topic + "'. Clock is now: " + currenVectorClock);

        MessageWrapper forward = createForwardMessage(chatMessage, currenVectorClock.asData());
        try {
            this.chatServerPubQueue.put(Pair.of(topic, forward));
        } catch (InterruptedException ignored) {}

        logsManager.addLog(forward.getForwardChatMessage());

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
