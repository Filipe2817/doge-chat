package com.doge.aggregation.server.handler;

import java.util.ArrayList;
import java.util.List;

import org.zeromq.ZContext;

import com.doge.aggregation.server.gossip.ChatServerState;
import com.doge.aggregation.server.gossip.GossipManager;
import com.doge.aggregation.server.socket.tcp.DhtClient;
import com.doge.common.Logger;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.AggregationCurrentStateMessage;
import com.doge.common.proto.AggregationResultMessage;
import com.doge.common.proto.ChatServerStateMessage;
import com.doge.common.proto.GetChatServerStateMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.proto.NotifyNewTopicAckMessage;
import com.doge.common.proto.NotifyNewTopicMessage;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;

public class AggregationCurrentStateMessageHandler implements MessageHandler<MessageWrapper> {
    private ZContext context;
    private RepEndpoint repEndpoint;
    private ReqEndpoint reqEndpoint;
    private DhtClient dhtClient;

    private GossipManager gossipManager;
    private Logger logger;

    public AggregationCurrentStateMessageHandler(
        ZContext context,
        RepEndpoint repEndpoint,
        ReqEndpoint reqEndpoint,
        DhtClient dhtClient,
        GossipManager gossipManager,
        Logger logger
    ) {
        this.context = context;
        this.repEndpoint = repEndpoint;
        this.reqEndpoint = reqEndpoint;
        this.dhtClient = dhtClient;

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
                List<ChatServerState> chosen = gossipManager.getBestChatServers(aggregationId);
                this.notifyInterestedParties(aggregationId, topic, chosen);
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

    private void notifyInterestedParties(
        String aggregationId, 
        String topic,
        List<ChatServerState> chosen
    ) {
        logger.info("Aggregation '" + aggregationId + "' was started by us. Notifying interested parties...");
        
        // Chosen chat servers
        List<Integer> chatServerIds = chosen.stream()
            .map(ChatServerState::id)
            .toList();

        for (ChatServerState chatServer : chosen) {
            List<Integer> otherChatServers = new ArrayList<>(chatServerIds);
            int selfId = chatServer.id();

            otherChatServers.removeIf(id -> id == selfId);
            this.notifyChatServer(selfId, topic, otherChatServers);
        }

        // Notify DHT
        dhtClient.create(topic, chatServerIds);

        // Client
        List<ChatServerState> result = gossipManager.getBestChatServers(aggregationId);
        MessageWrapper response = createAggregationResultMessage(topic, result);
        repEndpoint.send(response);
        logger.info("Notified client about aggregation result for topic '" + topic + "'");
    }

    private void notifyChatServer(
        int chatServerId,
        String topic,
        List<Integer> otherChatServers
    ) {
        MessageWrapper message = createNotifyNewTopicMessage(topic, otherChatServers);
        ReqEndpoint reqEndpoint = new ReqEndpoint(context);
        reqEndpoint.setLinger(2000);

        int repPort = chatServerId + 1;
        reqEndpoint.connectSocket("localhost", repPort);

        reqEndpoint.send(message);
        logger.info("Notified chat server " + chatServerId + " about new topic '" + topic + "'");

        try {
            MessageWrapper response = reqEndpoint.receiveOnceWithoutHandle();
            NotifyNewTopicAckMessage ack = response.getNotifyNewTopicAckMessage();

            logger.info("Received acknowledgment from chat server " + chatServerId + " for topic '" + ack.getTopic() + "'");
        } catch (InvalidFormatException ignored) {}

        reqEndpoint.close();
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

    private MessageWrapper createNotifyNewTopicMessage(String topic, List<Integer> otherChatServers) {
        NotifyNewTopicMessage message = NotifyNewTopicMessage.newBuilder()
            .setTopic(topic)
            .addAllOtherServerIds(otherChatServers)
            .build();

        return MessageWrapper.newBuilder()
            .setNotifyNewTopicMessage(message)
            .build();
    }
}
