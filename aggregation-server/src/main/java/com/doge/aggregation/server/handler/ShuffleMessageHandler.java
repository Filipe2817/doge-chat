package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;

import java.util.List;

import org.zeromq.ZContext;

import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.PeerEntry;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final AggregationServer aggregationServer;
    private final NeighbourManager neighbourManager;
    private final Integer l; // how many peer‑entries you send in the request

    private final PullEndpoint pullEndpoint;
    private final ZContext context;

    private final Logger logger;

    public ShuffleMessageHandler(
        AggregationServer aggregationServer,
        NeighbourManager neighbourManager,
        Integer l,
        PullEndpoint pullEndpoint,
        ZContext context,
        Logger logger
    ) {
        this.aggregationServer = aggregationServer;
        this.neighbourManager = neighbourManager;
        this.l = l;

        this.pullEndpoint = pullEndpoint;
        this.context = context;

        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        if (wrapper == null || !wrapper.hasShuffleMessage()) {
            logger.error("Received invalid message wrapper or missing shuffle message.");
            return;
        }

        ShuffleMessage shuffleMessage = wrapper.getShuffleMessage();
        List<PeerEntry> peerEntries = shuffleMessage.getEntriesList();
        Integer senderId = shuffleMessage.getSenderId();
        Integer type = shuffleMessage.getType();

        logger.info(String.format("Received shuffle message from '%d' with type '%d'", senderId, type));

        Integer selfId = aggregationServer.getId();
        Neighbour self = neighbourManager.get(selfId);
        if (self == null) {
            logger.error(String.format("Self neighbour with id '%d' not found.", selfId));
            return;
        }
        List<Neighbour> neighbours = neighbourManager.pickRandom(l - 1, self);
        neighbours.add(self);

        // Ensure sender is known regardless of message type.
        Neighbour sender = createNeighbour(senderId, 0);
        neighbourManager.addNeighbour(sender);
        for (PeerEntry peerEntry : peerEntries) {
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            // Skip self or already known neighbour.
            if (id.equals(selfId) || neighbourManager.get(id) != null) {
                continue;
            }

            Neighbour n = createNeighbour(id, age);
            neighbourManager.addNeighbour(n);
        }

        if (type == 1) { // For a shuffle request, send back a shuffle response with our neighbours.
            MessageWrapper shuffleResponse = createShuffleMessage(selfId, neighbours, 2);
            if (sender != null) {
                sender.sendMessage(shuffleResponse);
                logger.info(String.format("Sent shuffle response to neighbour '%d'", senderId));
            } else {
                logger.warn(String.format("Sender neighbour with id '%d' not found, unable to send response.", senderId));
            }
        }
    }

    public void triggerShuffle() {
        neighbourManager.ageAll();
        // If there is only self (or no neighbour) available, skip the shuffle
        if (neighbourManager.size() == 0) {
            logger.info("Not enough neighbours available to perform shuffle.");
            return;
        }
    
        Neighbour oldest = neighbourManager.getOldest();
        if (oldest == null) {
            logger.warn("No neighbour available to trigger shuffle.");
            return;
        }
    
        List<Neighbour> neighbours = neighbourManager.pickRandom(l - 1, oldest);
        neighbours.add(oldest);
    
        MessageWrapper shuffleMessage = createShuffleMessage(aggregationServer.getId(), neighbours, 1);
        oldest.sendMessage(shuffleMessage);
    }

    private MessageWrapper createShuffleMessage(Integer senderId, List<Neighbour> peerEntries, int type) {
        ShuffleMessage.Builder shuffleMessageBuilder = ShuffleMessage.newBuilder();
        shuffleMessageBuilder.setSenderId(senderId);
        shuffleMessageBuilder.setType(type);

        for (Neighbour neighbour : peerEntries) {
            int age = neighbour.getId().equals(senderId) ? 0 : neighbour.getAge(); // reset age for self
            PeerEntry peerEntry = PeerEntry.newBuilder()
            .setId(neighbour.getId())
            .setAge(age)
            .build();
            shuffleMessageBuilder.addEntries(peerEntry);
        }

        return MessageWrapper.newBuilder()
            .setShuffleMessage(shuffleMessageBuilder.build())
            .build();
    }

    private Neighbour createNeighbour(Integer id, Integer age) {
        PushEndpoint pushEndpoint = new PushEndpoint(this.context);
        pushEndpoint.connectSocket("localhost", id);
        return new Neighbour(id, this.pullEndpoint, pushEndpoint, age, this.logger);
    }
}