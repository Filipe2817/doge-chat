package com.doge.chat.server.causal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;

public class CausalDeliveryManager {
    private List<ForwardChatMessage> messagesBuffer;

    private final VectorClockManager vectorClockManager;
    private final PubEndpoint clientPubEndpoint;
    private Logger logger;

    public CausalDeliveryManager(VectorClockManager vectorClockManager, PubEndpoint clientPubEndpoint, Logger logger) {
        this.messagesBuffer = new ArrayList<>();

        this.vectorClockManager = vectorClockManager;
        this.clientPubEndpoint = clientPubEndpoint;
        this.logger = logger;
    }

    public void addAndMaybeDeliver(ForwardChatMessage message) {
        messagesBuffer.add(message);
        this.maybeDeliver();
    }

    private void maybeDeliver() {
        Iterator<ForwardChatMessage> it = messagesBuffer.iterator();
        
        while (it.hasNext()) {
            ForwardChatMessage forwardChatMessage = it.next();

            ChatMessage chatMessage = forwardChatMessage.getChatMessage();
            String topic = chatMessage.getTopic();
            int senderId = forwardChatMessage.getSenderId();

            VectorClock selfVectorClock = vectorClockManager.getByTopic(topic);
            VectorClock messageVectorClock = new VectorClock(forwardChatMessage.getVectorClockMap());

            String clientId = chatMessage.getClientId();
            if (canDeliver(selfVectorClock, messageVectorClock, senderId)) {
                logger.info("Delivering message from client '" + clientId + "' to topic '" + topic + "'");

                MessageWrapper wrapper = createChatMessageFromForwardChatMessage(forwardChatMessage);
                clientPubEndpoint.send(topic, wrapper);

                it.remove();
                vectorClockManager.incrementForTopic(topic, senderId);
                maybeDeliver();
            } else {
                logger.info("Message from client '" + clientId + "' to topic '" + topic + "' is not deliverable yet");
            }
        }
    }

    private boolean canDeliver(VectorClock selfVectorClock, VectorClock messageVectorClock, int senderId) {
        return selfVectorClock.isCausalDeliverable(messageVectorClock, senderId) || selfVectorClock.isConcurrent(messageVectorClock);
    }

    private MessageWrapper createChatMessageFromForwardChatMessage(ForwardChatMessage message) {
        return MessageWrapper.newBuilder()
                .setChatMessage(message.getChatMessage())
                .build();
    }
}
