package com.doge.chat.server.causal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.doge.chat.server.Logger;
import com.doge.chat.server.logs.LogsManager;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;

public class CausalDeliveryManager {
    private List<ForwardChatMessage> messagesBuffer;

    private final VectorClockManager vectorClockManager;
    private final PubEndpoint clientPubEndpoint;
    private Logger logger;
    private LogsManager logsManager;

    public CausalDeliveryManager(VectorClockManager vectorClockManager, PubEndpoint clientPubEndpoint, Logger logger,LogsManager logsManager) {
        this.messagesBuffer = new ArrayList<>();

        this.vectorClockManager = vectorClockManager;
        this.clientPubEndpoint = clientPubEndpoint;
        this.logger = logger;
        this.logsManager = logsManager;
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

            VectorClock selfVectorClock = this.vectorClockManager.getByTopic(topic);
            logger.debug("Self vector clock for topic '" + topic + "' is: " + selfVectorClock);

            VectorClock messageVectorClock = new VectorClock(forwardChatMessage.getVectorClockMap());
            logger.debug("Message vector clock for topic '" + topic + "' is: " + messageVectorClock);

            String clientId = chatMessage.getClientId();
            if (canDeliver(selfVectorClock, messageVectorClock, senderId)) {
                logger.info("Delivering message from client '" + clientId + "' to topic '" + topic + "'");

                //Add message to logs
                logsManager.addLog(forwardChatMessage);
                //logger.debug("Logs state: " + logsManager.getLogs());

                MessageWrapper wrapper = createChatMessageFromForwardChatMessage(forwardChatMessage);
                this.clientPubEndpoint.send(topic, wrapper);

                it.remove();
                this.vectorClockManager.incrementForTopic(topic, senderId);
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
