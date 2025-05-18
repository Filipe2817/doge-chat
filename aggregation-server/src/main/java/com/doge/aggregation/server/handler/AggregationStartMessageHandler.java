package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.gossip.ChatServerState;
import com.doge.aggregation.server.gossip.GossipManager;
import com.doge.common.Logger;
import com.doge.common.proto.AggregationStartMessage;
import com.doge.common.proto.ChatServerStateMessage;
import com.doge.common.proto.GetChatServerStateMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.ReqEndpoint;

public class AggregationStartMessageHandler implements MessageHandler<MessageWrapper> {
    private ReqEndpoint reqEndpoint;

    private GossipManager gossipManager;
    private Logger logger;

    public AggregationStartMessageHandler(
        ReqEndpoint reqEndpoint, 
        GossipManager gossipManager,
        Logger logger
    ) {
        this.reqEndpoint = reqEndpoint;

        this.gossipManager = gossipManager;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        AggregationStartMessage message = wrapper.getAggregationStartMessage();
        String dotClientId = message.getDotClientId();
        String topic = message.getTopic();

        logger.info("Received start aggregation request with dot id '" + dotClientId + "' on topic '" + topic + "'");

        String aggregationId = dotClientId;
        try {
            ChatServerState self = fetchSelfState();
            gossipManager.startAggregation(aggregationId, topic, self);
        } catch (Exception e) {
            logger.error("Failed to start aggregation: " + e.getMessage());
            return;
        }

        this.gossipManager.doGossipStep(aggregationId);
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
    
    private MessageWrapper createGetChatServerStateMessage() {
        GetChatServerStateMessage message = GetChatServerStateMessage.newBuilder().build();

        return MessageWrapper.newBuilder()
                .setGetChatServerStateMessage(message)
                .build();
    }
}
