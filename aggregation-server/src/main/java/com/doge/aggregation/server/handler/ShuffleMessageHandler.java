package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.ShuffleMessageType;
import com.doge.common.proto.PeerEntry;
import org.zeromq.ZContext;

import java.util.ArrayList;
import java.util.List;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final Integer l;
    private final AggregationServer aggregationServer;
    
    private final ZContext context;
    
    private final NeighbourManager neighbourManager;
    private final Logger logger;

    private List<Neighbour> candidates;
    private boolean shuffleInProgress;

    public ShuffleMessageHandler(
        Integer l,
        AggregationServer aggregationServer,
        ZContext context,
        NeighbourManager neighbourManager,
        Logger logger
    ) {
        this.l = l;
        this.aggregationServer = aggregationServer;

        this.context = context;

        this.neighbourManager = neighbourManager;
        this.logger = logger;

        this.candidates = List.of();
        this.shuffleInProgress = false;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        if (!wrapper.hasShuffleMessage()) {
            logger.error("Missing shuffle message in wrapper");
            return;
        }

        ShuffleMessage shuffleMessage = wrapper.getShuffleMessage();
        List<PeerEntry> peerEntries = shuffleMessage.getEntriesList();
        Integer senderId = shuffleMessage.getSenderId();
        ShuffleMessageType type = shuffleMessage.getType();

        logger.info("Received shuffle message from '" + senderId + "' with type " + type.name() + "");

        if (shuffleInProgress) {
            MessageWrapper busyMessage = createShuffleMessage(senderId, List.of(), ShuffleMessageType.BUSY);
            Neighbour sender = new Neighbour(senderId, new PushEndpoint(this.context), 0, this.logger);

            sender.connect();
            sender.sendMessage(busyMessage);
            sender.disconnect();
            
            logger.warn("Shuffle already in progress, ignoring new shuffle message.");
            return;
        }

        if (type == ShuffleMessageType.REQUEST) {
            this.handleShuffleRequest(senderId, peerEntries);
        } else if (type == ShuffleMessageType.RESPONSE) {
            this.handleShuffleResponse(senderId, peerEntries);
        } else if (type == ShuffleMessageType.BUSY) {
            logger.warn("Received BUSY message from " + senderId);
            return;
        } else {
            logger.error("Unknown shuffle message type: " + type);
            return;
        }
    }

    public void handleShuffleRequest(Integer senderId, List<PeerEntry> peerEntries) {
        List<Neighbour> randomCandidates = neighbourManager.pickRandom(l, null);

        // Use an index pointer for removal to avoid out-of-bound errors
        int removalCandidateIndex = 0;

        for (PeerEntry peerEntry : peerEntries) {
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            // Process sender entry: update or add sender to the neighbour manager
            if (id.equals(senderId)) {
                Neighbour sender = neighbourManager.get(senderId);
                if (sender == null) {
                    Neighbour newSender = new Neighbour(senderId, new PushEndpoint(this.context), 0, this.logger);
                    newSender.connect();
                    neighbourManager.addNeighbour(newSender);
                } else {
                    sender.resetAge();
                }

                continue;
            }
            
            // Skip if the entry corresponds to self or is already known
            if (id.equals(aggregationServer.getId()) || neighbourManager.get(id) != null) {
                continue;
            }

            Neighbour newNeighbour = new Neighbour(id, new PushEndpoint(this.context), age, this.logger);
            newNeighbour.connect();
            
            // If the neighbour manager is full, remove a candidate safely from randomCandidates
            if (neighbourManager.isFull() && !randomCandidates.isEmpty()) {
                // Ensure we do not go out of bounds
                if (removalCandidateIndex >= randomCandidates.size()) {
                    removalCandidateIndex = randomCandidates.size() - 1;
                }

                Neighbour candidateToRemove = randomCandidates.get(removalCandidateIndex);
                candidateToRemove.disconnect();
                removalCandidateIndex++;
                neighbourManager.remove(candidateToRemove.getId());
            }
            
            neighbourManager.addNeighbour(newNeighbour);
        }

        // Prepare and send back the shuffle response
        MessageWrapper shuffleResponse = createShuffleMessage(aggregationServer.getId(), randomCandidates, ShuffleMessageType.RESPONSE);
        Neighbour sender = neighbourManager.get(senderId);
        if (sender != null) {
            try {
                sender.sendMessage(shuffleResponse);
                logger.info("Sent shuffle response to neighbour '" + senderId + "'");
            } catch (Exception e) {
                logger.error("Failed to send shuffle response, neighbour may be unreachable: " + e.getMessage());
            }
        } else {
            logger.warn("Sender neighbour with id '" + senderId + "' not found, unable to send response.");
        }
    }

    public void handleShuffleResponse(Integer senderId, List<PeerEntry> peerEntries) {
        // Separate index for candidates to remove
        int removalCandidateIndex = 0;
        for (int i = 0; i < peerEntries.size(); i++) {
            PeerEntry peerEntry = peerEntries.get(i);
            Integer id = peerEntry.getId();
            Integer age = peerEntry.getAge();

            if (id.equals(aggregationServer.getId()) || neighbourManager.get(id) != null) {
                continue;
            }

            Neighbour newNeighbour = new Neighbour(id, new PushEndpoint(this.context), age, this.logger);
            newNeighbour.connect();
            
            if (neighbourManager.isFull()) {
                // Adjust index safely
                if (removalCandidateIndex >= this.candidates.size()) {
                    removalCandidateIndex = this.candidates.size() - 1;
                }

                Neighbour removed = this.candidates.get(removalCandidateIndex);
                removed.disconnect();
                neighbourManager.remove(removed.getId());
                removalCandidateIndex++;
            }
            
            neighbourManager.addNeighbour(newNeighbour);
        }

        this.candidates = List.of();
        this.shuffleInProgress = false;
    }

    public void triggerShuffle() {
        try {
            neighbourManager.ageAll();
            this.shuffleInProgress = true;

            if (neighbourManager.size() == 0) {
                logger.info("Not enough neighbours available to perform shuffle");
                return;
            }

            Neighbour oldest = neighbourManager.getOldest();
            if (oldest == null) {
                logger.error("No oldest neighbour found");
                return;
            }

            logger.info("Triggering shuffle with neighbour '" + oldest.getId() + "'");
            neighbourManager.remove(oldest.getId());

            this.candidates = neighbourManager.pickRandom(l - 1, oldest);
            List<Neighbour> candidatesToSend = new ArrayList<>(candidates);

            Neighbour self = new Neighbour(aggregationServer.getId(), new PushEndpoint(this.context), 0, this.logger);
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
            // Reset age for self
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
}
