package com.doge.aggregation.server.gossip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.doge.aggregation.server.neighbour.Neighbour;
import com.doge.aggregation.server.neighbour.NeighbourManager;
import com.doge.common.Logger;
import com.doge.common.proto.AggregationCurrentStateMessage;
import com.doge.common.proto.ChatServerStateMessage;
import com.doge.common.proto.MessageWrapper;

public class GossipManager {
    // Each aggregation will gather the best C chat servers
    private static final int C = 2;

    // If the list stays the same for T rounds, the aggregation will be
    // considered finished
    private static final int T = 5;

    // Map from aggregationId to record `AggregationState`
    //
    // aggregationId = dotted clientId that started the aggregation
    private Map<String, AggregationState> aggregations;

    private NeighbourManager neighbourManager;
    private Logger logger;

    public GossipManager(NeighbourManager neighbourManager, Logger logger) {
        this.aggregations = new HashMap<>();

        this.neighbourManager = neighbourManager;
        this.logger = logger;
    }

    public void startAggregation(String aggregationId, String topic, ChatServerState chatServerState) {
        List<ChatServerState> chatServers = new ArrayList<>();
        chatServers.add(chatServerState);

        AggregationState state = new AggregationState(topic, true, false, 0, chatServers);
        this.aggregations.put(aggregationId, state);
        logger.info("Initialized (self) aggregation with id '" + aggregationId + "'");
    }

    public void startAggregationWithBatch(String aggregationId, String topic, List<ChatServerState> batch) {
        TreeSet<ChatServerState> sorted = new TreeSet<>(ChatServerState::compareTo);
        sorted.addAll(batch);

        List<ChatServerState> truncated = sorted.stream()
            .limit(C)
            .collect(Collectors.toList());

        aggregations.put(aggregationId, new AggregationState(topic, false, false, 0, truncated));
        logger.info("Initialized (not self) aggregation '" + aggregationId + "'");
    }

    public boolean hasAggregationStarted(String aggregationId) {
        return aggregations.containsKey(aggregationId);
    }

    public void addChatServerState(String aggregationId, ChatServerState incoming) {
        AggregationState old = this.aggregations.get(aggregationId);
        List<ChatServerState> current = new ArrayList<>(old.chatServers());
        int tValue = old.t();

        Set<Integer> previousIds = current.stream()
            .map(ChatServerState::id)
            .collect(Collectors.toSet());

        TreeSet<ChatServerState> sorted = new TreeSet<>(ChatServerState::compareTo);
        sorted.addAll(current);
        sorted.add(incoming);

        List<ChatServerState> truncated = sorted.stream()
            .limit(C)
            .collect(Collectors.toList());

        Set<Integer> newIds = truncated.stream()
            .map(ChatServerState::id)
            .collect(Collectors.toSet());

        boolean changed = !newIds.equals(previousIds);
        if (truncated.size() < C || changed) {
            tValue = 0;
        } else {
            tValue++;
        }

        boolean startedByUs = old.startedByUs();
        aggregations.put(aggregationId, new AggregationState(
            old.topic(),
            startedByUs,
            false,
            tValue,
            truncated
        ));

        int threshold = startedByUs ? T : 2*T;
        if (startedByUs) {
            logger.info("T is now (self) " + tValue + "/" + threshold + " for aggregation '" + aggregationId + "'");
        } else {
            logger.info("T is now (not self) " + tValue + "/" + threshold + " for aggregation '" + aggregationId + "'");
        }
    }

    public boolean wasStartedByUs(String aggregationId) {
        AggregationState state = aggregations.get(aggregationId);
        if (state == null) return false;

        return state.startedByUs();
    }

    public boolean hasAggregationFinished(String aggregationId) {
        AggregationState state = aggregations.get(aggregationId);
        if (state == null) return false;

        int threshold = state.startedByUs() ? T : T + 2*T;
        return state.finished() || state.t() >= threshold;
    }

    public void finishAggregation(String aggregationId) {
        AggregationState old = aggregations.get(aggregationId);
        if (old == null) return;

        boolean startedByUs = old.startedByUs();
        aggregations.put(
            aggregationId,
            new AggregationState(
                old.topic(),
                startedByUs, 
                true, 
                old.t(), 
                old.chatServers()
            )
        );

        if (startedByUs) {
            logger.info("Finished aggregation '" + aggregationId + "' (self)");
            logger.warn("Best C chat servers: " + old.chatServers());
        } else {
            logger.info("Finished aggregation '" + aggregationId + "' (not self)");
            logger.warn("Best C chat servers: " + old.chatServers());
        }
    }

    public List<ChatServerState> getBestChatServers(String aggregationId) {
        AggregationState s = aggregations.get(aggregationId);
        return s == null ? new ArrayList<>() : s.chatServers();
    }
    
    /* Messaging & Gossiping */

    public void doGossipStep(String aggregationId) {
        AggregationState state = this.aggregations.get(aggregationId);
        List<ChatServerState> bestChatServers = this.getBestChatServers(aggregationId);
        if (bestChatServers.isEmpty()) {
            return;
        }

        MessageWrapper message = this.createAggregationCurrentStateMessage(
            aggregationId,
            state.topic(),
            bestChatServers
        );

        Iterator<Neighbour> it = this.neighbourManager.iterator();
        while (it.hasNext()) {
            Neighbour neighbour = it.next();
            logger.info("Gossiping to neighbour '" + neighbour.getId() + "'");
            neighbour.sendMessage(message);
        } 
    }

    private MessageWrapper createAggregationCurrentStateMessage(
        String id,
        String topic,
        List<ChatServerState> bestChatServers
    ) {
        AggregationCurrentStateMessage.Builder builder = AggregationCurrentStateMessage.newBuilder()
                .setAggregationId(id)
                .setTopic(topic);

        for (ChatServerState state : bestChatServers) {
            ChatServerStateMessage stateMessage = ChatServerStateMessage.newBuilder()
                    .setId(state.id())
                    .setUserCount(state.userCount())
                    .setTopicCount(state.topicCount())
                    .build();

            builder.addChatServerStates(stateMessage);
        }

        return MessageWrapper.newBuilder()
                .setAggregationCurrentStateMessage(builder.build())
                .build();
    }
}
