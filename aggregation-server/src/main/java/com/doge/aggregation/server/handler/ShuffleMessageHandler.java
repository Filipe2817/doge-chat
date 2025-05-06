package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.ShuffleMessage;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final AggregationServer aggregationServer;
    private final Logger logger;

    private final PullEndpoint pullEndpoint;
    private final NeighbourManager neighbourManager;

    public ShuffleMessageHandler(
        AggregationServer aggregationServer,
        PullEndpoint pullEndpoint,
        NeighbourManager neighbourManager,
        Logger logger
    ) {
        this.aggregationServer = aggregationServer;
        this.logger = logger;

        this.pullEndpoint = pullEndpoint;
        this.neighbourManager = neighbourManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        // logic here ...
    }
    
}