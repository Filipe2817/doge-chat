package com.doge.aggregation.server.cyclon;

import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbour.Neighbour;
import com.doge.aggregation.server.neighbour.NeighbourManager;
import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.zmq.PushEndpoint;
import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.ShuffleMessageType;
import com.doge.common.proto.PeerEntry;
import com.doge.common.proto.RandomWalkMessage;
import com.doge.common.proto.RandomWalkMessageType;

import org.zeromq.ZContext;

import java.util.ArrayList;
import java.util.List;

public class CyclonManager {
    private final Integer l;
    private final AggregationServer aggregationServer;
    
    private final ZContext context;
    
    private final NeighbourManager neighbourManager;
    private final Logger logger;

    private List<Neighbour> candidates;

    public CyclonManager(
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

        this.candidates = new ArrayList<>();
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

        MessageWrapper shuffleResponse = createShuffleMessage(aggregationServer.getId(), randomCandidates, ShuffleMessageType.SHUFFLE_RESPONSE);
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
                // Ensure candidates list is not empty before accessing
                if (!this.candidates.isEmpty()) {
                    if (removalCandidateIndex >= this.candidates.size()) {
                        removalCandidateIndex = this.candidates.size() - 1;
                    }

                    Neighbour removed = this.candidates.get(removalCandidateIndex);
                    removed.disconnect();
                    neighbourManager.remove(removed.getId());
                    removalCandidateIndex++;
                }
            }
            
            neighbourManager.addNeighbour(newNeighbour);
        }

        this.candidates = new ArrayList<>();
    }

    public void handleRandomWalkRequest(Integer senderId, int ttl) {
        List<Neighbour> neighbours = neighbourManager.getAll();

        if (neighbours.isEmpty()) {
            // If there are no neighbours yet, send a response with self in it
            Neighbour sender = new Neighbour(senderId, new PushEndpoint(this.context), 0, this.logger);
            sender.connect();

            PeerEntry peerEntry = PeerEntry.newBuilder()
                .setId(aggregationServer.getId())
                .setAge(0)
                .build();
            List<PeerEntry> peerEntries = new ArrayList<>();
            peerEntries.add(peerEntry);

            MessageWrapper randomWalkMessage = createRandomWalkMessage(aggregationServer.getId(), peerEntries, 0, RandomWalkMessageType.RANDOM_WALK_RESPONSE);
            sender.sendMessage(randomWalkMessage);
            sender.disconnect();
        } else {
            for (Neighbour neighbour : neighbours) {
                MessageWrapper randomWalkMessage = createRandomWalkMessage(
                    senderId, 
                    new ArrayList<>(), 
                    ttl, 
                    RandomWalkMessageType.RANDOM_WALK_INTRO
                );
                neighbour.sendMessage(randomWalkMessage);
            }
        }
    }

    public void handleRandomWalkIntro(Integer senderId, int ttl) {
        if (ttl > 0 && neighbourManager.size() > 0) {
            Neighbour next = neighbourManager.getRandom();
            if (next != null) {
                MessageWrapper randomWalkMessage = createRandomWalkMessage(
                    senderId, 
                    new ArrayList<>(),
                    ttl - 1, 
                    RandomWalkMessageType.RANDOM_WALK_INTRO
                );
                next.sendMessage(randomWalkMessage);
            }
        } else {
            Neighbour newNeighbour = new Neighbour(senderId, new PushEndpoint(this.context), 0, this.logger);
            Neighbour candidate = neighbourManager.getRandom();
            if (candidate == null) {
                // If we have no entries in our cache, set the candidate to self
                candidate = new Neighbour(aggregationServer.getId(), null, 0, this.logger);
            } else {
                neighbourManager.remove(candidate.getId());
                candidate.disconnect();
            }
            
            neighbourManager.addNeighbour(newNeighbour);
            newNeighbour.connect();

            PeerEntry peerEntry = PeerEntry.newBuilder()
                .setId(candidate.getId())
                .setAge(candidate.getAge())
                .build();
            List<PeerEntry> peerEntries = new ArrayList<>();
            peerEntries.add(peerEntry);
    
            MessageWrapper randomWalkMessage = createRandomWalkMessage(aggregationServer.getId(), peerEntries, 0, RandomWalkMessageType.RANDOM_WALK_RESPONSE);
            newNeighbour.sendMessage(randomWalkMessage);
        }
    }

    public void handleRandomWalkResponse(Integer senderId, List<PeerEntry> peerEntries) {
        try {
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
                    // Ensure candidates list is not empty before accessing
                    if (!this.candidates.isEmpty()) {
                        if (removalCandidateIndex >= this.candidates.size()) {
                            removalCandidateIndex = this.candidates.size() - 1;
                        }

                        Neighbour removed = this.candidates.get(removalCandidateIndex);
                        removed.disconnect();
                        neighbourManager.remove(removed.getId());
                        removalCandidateIndex++;
                    }
                }
                
                neighbourManager.addNeighbour(newNeighbour);
            }

            this.candidates = new ArrayList<>();
        } catch (Exception e) {
            logger.error("Failed to handle random walk response: " + e.getMessage());
        }
    }

    public void triggerShuffle() {
        try {
            neighbourManager.ageAll();

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

            MessageWrapper shuffleMessage = createShuffleMessage(aggregationServer.getId(), candidatesToSend, ShuffleMessageType.SHUFFLE_REQUEST);
            oldest.sendMessage(shuffleMessage);
            oldest.disconnect();
        } catch (Exception e) {
            logger.error("Failed to send shuffle message, neighbour may be unreachable: " + e.getMessage());
        }
    }

    public void triggerRandomWalk() {
        try {
            if (neighbourManager.size() == 0) {
                logger.info("Not enough neighbours available to perform shuffle. At least one neighbour is required.");
                return;
            }

            Neighbour oldest = neighbourManager.getOldest();
            MessageWrapper randomWalkMessage = createRandomWalkMessage(
                aggregationServer.getId(), 
                new ArrayList<>(),
                neighbourManager.cacheSize(), 
                RandomWalkMessageType.RANDOM_WALK_REQUEST
            );
            oldest.sendMessage(randomWalkMessage);
        } catch (Exception e) {
            logger.error("Failed to send shuffle message, neighbour may be unreachable: " + e.getMessage());
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

    private MessageWrapper createRandomWalkMessage(Integer senderId, List<PeerEntry> peerEntries, int ttl, RandomWalkMessageType type) {
        RandomWalkMessage.Builder randomWalkMessageBuilder = RandomWalkMessage.newBuilder();
        randomWalkMessageBuilder.setSenderId(senderId);
        randomWalkMessageBuilder.setType(type);
        randomWalkMessageBuilder.setTtl(ttl);

        for (PeerEntry peerEntry : peerEntries) {
            randomWalkMessageBuilder.addEntries(peerEntry);
        }

        return MessageWrapper.newBuilder()
            .setRandomWalkMessage(randomWalkMessageBuilder.build())
            .build();
    }
}
