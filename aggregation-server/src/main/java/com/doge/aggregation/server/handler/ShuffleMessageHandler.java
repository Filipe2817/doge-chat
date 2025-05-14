package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

import zmq.socket.Peer;

import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.ShuffleMessageType;
import com.doge.common.proto.PeerEntry;
import org.zeromq.ZContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final AggregationServer aggregationServer;
    private final NeighbourManager neighbourManager;
    private final Integer l; // number of peer-entries to send in the request
    private final PullEndpoint pullEndpoint;
    private final ZContext context;
    private final Logger logger;

    private List<Neighbour> candidates;
    private boolean shuffleInProgress;

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

        this.candidates = List.of();
        this.shuffleInProgress = false;
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

        if (shuffleInProgress) {
            MessageWrapper busyMessage = createShuffleMessage(senderId, List.of(), ShuffleMessageType.BUSY);
            Neighbour sender = createNeighbour(senderId, 0);
            sender.sendMessage(busyMessage);
            
            logger.warn("Shuffle already in progress, ignoring new shuffle message.");
            return;
        }

        logger.info(String.format("Received shuffle message from '%d' with type '%s'",
            senderId, type.name()));

        if (type == ShuffleMessageType.REQUEST) {
            this.handleShuffleRequest(senderId, peerEntries);
        } else if (type == ShuffleMessageType.RESPONSE) {
            this.handleShuffleResponse(senderId, peerEntries);
        } else if (type == ShuffleMessageType.BUSY) {
            logger.warn("Received busy message from " + senderId);
            return;
        } else {
            logger.error("Unknown shuffle message type: " + type);
            return;
        }
    }

    public void handleShuffleRequest(Integer senderId, List<PeerEntry> peerEntries) {
        // Pick random neighbours to exchange with
        List<Neighbour> randomCandidates = neighbourManager.pickRandom(l, null);
        // Use an index pointer for removal to avoid out-of-bound errors
        int removalCandidateIndex = 0;

        for (PeerEntry peerEntry : peerEntries) {
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            // Process sender entry: update or add sender to the neighbour manager.
            if (id.equals(senderId)) {
                Neighbour sender = neighbourManager.get(senderId);
                if (sender == null) {
                    Neighbour newSender = createNeighbour(senderId, 0);
                    neighbourManager.addNeighbour(newSender);
                } else {
                    sender.resetAge();
                }
                continue;
            }
            
            // Skip if the entry corresponds to self or is already known.
            if (id.equals(aggregationServer.getId()) || neighbourManager.get(id) != null) {
                continue;
            }

            Neighbour newNeighbour = createNeighbour(id, age);
            
            // If the neighbour manager is full, remove a candidate safely from randomCandidates.
            if (neighbourManager.isFull() && !randomCandidates.isEmpty()) {
                // Ensure we do not go out of bounds.
                if (removalCandidateIndex >= randomCandidates.size()) {
                    removalCandidateIndex = randomCandidates.size() - 1;
                }
                Neighbour candidateToRemove = randomCandidates.get(removalCandidateIndex);
                removalCandidateIndex++; // Increment for subsequent removals.
                neighbourManager.remove(candidateToRemove.getId());
            }
            
            neighbourManager.addNeighbour(newNeighbour);
        }

        // Prepare and send back the shuffle response.
        MessageWrapper shuffleResponse = createShuffleMessage(aggregationServer.getId(), randomCandidates, ShuffleMessageType.RESPONSE);
        Neighbour sender = neighbourManager.get(senderId);
        if (sender != null) {
            try {
                sender.sendMessage(shuffleResponse);
                logger.info(String.format("Sent shuffle response to neighbour '%d'", senderId));
            } catch (Exception e) {
                logger.error("Failed to send shuffle response, neighbour may be unreachable: " + e.getMessage());
            }
        } else {
            logger.warn(String.format("Sender neighbour with id '%d' not found, unable to send response.", senderId));
        }
    }

    public void handleShuffleResponse(Integer senderId, List<PeerEntry> peerEntries) {
        int removalCandidateIndex = 0; // separate index for candidates removal
        for (int i = 0; i < peerEntries.size(); i++) {
            PeerEntry peerEntry = peerEntries.get(i);
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            if (id.equals(aggregationServer.getId()) || neighbourManager.get(id) != null) {
                continue;
            }
            Neighbour newNeighbour = createNeighbour(id, age);
            
            if (neighbourManager.isFull()) {
                if (removalCandidateIndex >= this.candidates.size()) {
                    removalCandidateIndex = this.candidates.size() - 1; // adjust index safely
                }
                Neighbour removed = this.candidates.get(removalCandidateIndex);
                neighbourManager.remove(removed.getId());
                removalCandidateIndex++;
            }
            
            neighbourManager.addNeighbour(newNeighbour);
        }

        this.candidates = List.of(); // Clear the candidates list after processing the response.
        this.shuffleInProgress = false;
    }

    public void triggerShuffle() {
        try {
            neighbourManager.ageAll();
            this.shuffleInProgress = true;

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

            this.candidates = neighbourManager.pickRandom(l - 1, oldest); // Exclude the self from the candidates.
            List<Neighbour> candidatesToSend = new ArrayList<>(candidates); // Copy the candidates to send with self

            Neighbour self = createSelfNeighbour(aggregationServer.getId());
            candidatesToSend.add(self);

            MessageWrapper shuffleMessage = createShuffleMessage(aggregationServer.getId(), candidatesToSend, ShuffleMessageType.REQUEST);
            oldest.sendMessage(shuffleMessage);
        } catch (Exception e) {
            this.shuffleInProgress = false;
            logger.error("Failed to send shuffle message, neighbour may be unreachable: " + e.getMessage());
        } finally {
            this.shuffleInProgress = false;
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