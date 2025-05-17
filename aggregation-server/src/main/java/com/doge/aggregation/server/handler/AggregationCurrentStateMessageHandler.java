package com.doge.aggregation.server.handler;

import java.util.ArrayList;
import java.util.List;

import com.doge.aggregation.server.gossip.ChatServerState;
import com.doge.aggregation.server.gossip.GossipManager;
import com.doge.common.Logger;
import com.doge.common.proto.AggregationCurrentStateMessage;
import com.doge.common.proto.AggregationResultMessage;
import com.doge.common.proto.ChatServerStateMessage;
import com.doge.common.proto.GetChatServerStateMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;

public class AggregationCurrentStateMessageHandler implements MessageHandler<MessageWrapper> {
    private RepEndpoint repEndpoint;
    private ReqEndpoint reqEndpoint;

    private GossipManager gossipManager;
    private Logger logger;

    public AggregationCurrentStateMessageHandler(
        RepEndpoint repEndpoint,
        ReqEndpoint reqEndpoint,
        GossipManager gossipManager,
        Logger logger
    ) {
        this.repEndpoint = repEndpoint;
        this.reqEndpoint = reqEndpoint;

        this.gossipManager = gossipManager;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        AggregationCurrentStateMessage message = wrapper.getAggregationCurrentStateMessage();
        String aggregationId = message.getAggregationId();
        String topic = message.getTopic();

        List<ChatServerState> incomingStates = buildChatServerStateList(message);
        logger.info("Received current state for aggregation '" + aggregationId + "'");

        try {
            if (!this.gossipManager.hasAggregationStarted(aggregationId)) {
                logger.info("First-time aggregation '" + aggregationId + "' on remote. Seeding batch...");

                ChatServerState selfState = fetchSelfState();
                List<ChatServerState> seed = new ArrayList<>();
                seed.add(selfState);
                seed.addAll(incomingStates);
                gossipManager.startAggregationWithBatch(aggregationId, topic, seed);
                
                gossipManager.doGossipStep(aggregationId);
                return;
            }

            if (gossipManager.hasAggregationFinished(aggregationId)) {
                logger.info("Aggregation '" + aggregationId + "' has already finished. Ignoring current state message...");
                return;
            }

            this.getAndUpdateChatServerStateWithNew(aggregationId, incomingStates);
        } catch (Exception e) {
            logger.error("Failed to get chat server state: " + e.getMessage());
            return;
        }

        if (this.gossipManager.hasAggregationFinished(aggregationId)) {
            gossipManager.finishAggregation(aggregationId);

            if (gossipManager.wasStartedByUs(aggregationId)) {
                this.notifyInterestedParties(aggregationId, topic);
            }
        } else {
            gossipManager.doGossipStep(aggregationId);
        }
    }

    private ChatServerState fetchSelfState() throws Exception {
        logger.info("Requesting self chat server state...");
        MessageWrapper request = createGetChatServerStateMessage();
        reqEndpoint.send(request);

        MessageWrapper response = reqEndpoint.receiveOnceWithoutHandle();
        ChatServerStateMessage message = response.getChatServerStateMessage();
        return new ChatServerState(
            message.getId(),
            message.getUserCount(),
            message.getTopicCount()
        );
    }
    
    private void getAndUpdateChatServerStateWithNew(
        String aggregationId, 
        List<ChatServerState> incomingStates
    ) throws Exception {
        ChatServerState selfState = fetchSelfState();
        gossipManager.addChatServerState(aggregationId, selfState);
        logger.info("Added self state for aggregation '" + aggregationId + "': " + selfState);

        for (ChatServerState chatServer : incomingStates) {
            gossipManager.addChatServerState(aggregationId, chatServer);
            logger.info("Added incoming chat server state (" + chatServer.id() + ") for aggregation '" + aggregationId + "'");
        }
    }

    private void notifyInterestedParties(String aggregationId, String topic) {
        logger.info("Aggregation '" + aggregationId + "' was started by us. Sending result to DHT and client...");

        // TODO: Send to DHT

        // Client
        List<ChatServerState> result = gossipManager.getBestChatServers(aggregationId);
        MessageWrapper response = createAggregationResultMessage(topic, result);
        repEndpoint.send(response);
    }

    private MessageWrapper createGetChatServerStateMessage() {
        GetChatServerStateMessage message = GetChatServerStateMessage.newBuilder().build();

        return MessageWrapper.newBuilder()
                .setGetChatServerStateMessage(message)
                .build();
    }

    private List<ChatServerState> buildChatServerStateList(AggregationCurrentStateMessage message) {
        List<ChatServerState> chatServers = new ArrayList<>();

        for (ChatServerStateMessage chatServer : message.getChatServerStatesList()) {
            chatServers.add(new ChatServerState(
                chatServer.getId(),
                chatServer.getUserCount(),
                chatServer.getTopicCount()
            ));
        }

        return chatServers;
    }

    private MessageWrapper createAggregationResultMessage(String topic, List<ChatServerState> chatServers) {
        List<Integer> ids = chatServers.stream()
            .map(ChatServerState::id)
            .toList();
        
        AggregationResultMessage message = AggregationResultMessage.newBuilder()
            .setTopic(topic)
            .addAllServerIds(ids)
            .build();

        return MessageWrapper.newBuilder()
            .setAggregationResultMessage(message)
            .build();
    }
}
