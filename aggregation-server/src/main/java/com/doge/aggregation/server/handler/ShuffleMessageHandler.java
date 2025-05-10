package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.ShuffleMessageType;
import com.doge.common.proto.PeerEntry;
import org.zeromq.ZContext;
import java.util.List;
import java.util.stream.Collectors;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final AggregationServer aggregationServer;
    private final NeighbourManager neighbourManager;
    private final Integer l; // number of peer-entries to send in the request
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
        ShuffleMessageType type = shuffleMessage.getType();

        logger.info(String.format("Received shuffle message from '%d' with type '%s'",
            senderId, type.name()));

        final int selfId = aggregationServer.getId();

        // Pick random neighbours from the manager and explicitly remove any with our selfId.
        List<Neighbour> candidates = neighbourManager.pickRandom(l, null);
        List<Neighbour> neighbours = candidates.stream()
            .filter(n -> !n.getId().equals(selfId))
            .collect(Collectors.toList());

        // Ensure the sender is known; if not, add it.
        Neighbour sender = createNeighbour(senderId, 0);
        neighbourManager.addNeighbour(sender);

        for (PeerEntry peerEntry : peerEntries) {
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            // Skip our own id or if we already know this neighbour.
            if (id.equals(selfId) || neighbourManager.get(id) != null) {
                continue;
            }
            Neighbour n = createNeighbour(id, age);
            neighbourManager.addNeighbour(n);
        }

        if (type == ShuffleMessageType.REQUEST) { // shuffle request => send back a response
            MessageWrapper shuffleResponse = createShuffleMessage(selfId, neighbours, ShuffleMessageType.RESPONSE);
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
        if (neighbourManager.size() == 0) {
            logger.info("Not enough neighbours available to perform shuffle.");
            return;
        }

        Neighbour oldest = neighbourManager.getOldest();
        if (oldest == null) {
            logger.error("No oldest neighbour found.");
            return;
        }
        logger.info("Triggering shuffle with neighbour: " + oldest.getId());
        neighbourManager.remove(oldest.getId());

        List<Neighbour> candidates = neighbourManager.pickRandom(l - 1, oldest);
        Neighbour self = createSelfNeighbour(aggregationServer.getId());
        candidates.add(self);

        MessageWrapper shuffleMessage = createShuffleMessage(aggregationServer.getId(), candidates, ShuffleMessageType.REQUEST);
        try {
            oldest.sendMessage(shuffleMessage);
        } catch (Exception e) {
            logger.error("Failed to send shuffle message, neighbour may be unreachable: " + e.getMessage());
        }
    }

    private MessageWrapper createShuffleMessage(Integer senderId, List<Neighbour> peerEntries, ShuffleMessageType type) {
        ShuffleMessage.Builder shuffleMessageBuilder = ShuffleMessage.newBuilder();
        shuffleMessageBuilder.setSenderId(senderId);
        shuffleMessageBuilder.setType(type);

        for (Neighbour neighbour : peerEntries) {
            // Reset age for self.
            int age = neighbour.getId().equals(senderId) ? 0 : neighbour.getAge();
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
        // Assume the connection uses "localhost" and id as port or part of addressing
        pushEndpoint.connectSocket("localhost", id);
        return new Neighbour(id, this.pullEndpoint, pushEndpoint, age, this.logger);
    }

    // Create a self neighbour (not stored in the manager) when needed.
    private Neighbour createSelfNeighbour(int id) {
        PushEndpoint pushEndpoint = new PushEndpoint(this.context);
        pushEndpoint.connectSocket("localhost", id);
        return new Neighbour(id, this.pullEndpoint, pushEndpoint, 0, this.logger);
    }
}