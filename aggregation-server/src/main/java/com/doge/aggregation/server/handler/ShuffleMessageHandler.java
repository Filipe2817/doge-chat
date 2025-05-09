package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;

import java.util.List;

import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.PeerEntry;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final AggregationServer aggregationServer;
    private final Logger logger;

    private final PullEndpoint pullEndpoint;
    private final NeighbourManager neighbourManager;

    private final Integer l; // how many peer‑entries you send in the request

    public ShuffleMessageHandler(
        AggregationServer aggregationServer,
        PullEndpoint pullEndpoint,
        NeighbourManager neighbourManager,
        Integer l,
        Logger logger
    ) {
        this.aggregationServer = aggregationServer;

        this.pullEndpoint = pullEndpoint;
        this.neighbourManager = neighbourManager;

        this.l = l;

        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        // logic here ...
        ShuffleMessage shuffleMessage = wrapper.getShuffleMessage();
        // extract the PeerEntry list from the shuffle message
        List<PeerEntry> peerEntries = shuffleMessage.getEntriesList();
        Integer senderId = shuffleMessage.getSenderId();

        logger.info("Received shuffle message from '" + senderId);

        neighbourManager.addNeighbour(senderId);
        for (PeerEntry peerEntry : peerEntries) {
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            neighbourManager.addNeighbour(id, age);
        }

        Integer selfId = aggregationServer.getId();
        List<Neighbour> neighbours = neighbourManager.pickRandom(l);

        MessageWrapper shuffleResponse = createShuffleMessage(selfId, neighbours);
        Neighbour sender = neighbourManager.get(senderId);
        sender.sendMessage(shuffleResponse);
    }

    private MessageWrapper createShuffleMessage(Integer senderId, List<Neighbour> peerEntries) {
        ShuffleMessage.Builder shuffleMessageBuilder = ShuffleMessage.newBuilder();
        shuffleMessageBuilder.setSenderId(senderId);

        for (Neighbour neighbour : peerEntries) {
            PeerEntry peerEntry = PeerEntry.newBuilder()
                .setId(neighbour.getId())
                .setAge(neighbour.getAge())
                .build();
            shuffleMessageBuilder.addEntries(peerEntry);
        }

        return MessageWrapper.newBuilder()
            .setShuffleMessage(shuffleMessageBuilder.build())
            .build();
    }
}