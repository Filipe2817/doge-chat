package com.doge.chat.server.causal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorClockManager {
    private @ServerIdType int selfIdentifier;

    // TODO: Should this be a ConcurrentHashMap?
    private Map<String, VectorClock> vectorClocksPerTopic;

    public VectorClockManager(@ServerIdType int selfIdentifier) {
        this.selfIdentifier = selfIdentifier;
        this.vectorClocksPerTopic = new HashMap<>();
    }

    public VectorClock getByTopic(String topic) {
        return vectorClocksPerTopic.get(topic);
    }

    public void addTopic(String topic, List<Integer> servers) {
        VectorClock vectorClock = new VectorClock(servers);
        vectorClocksPerTopic.put(topic, vectorClock);
    }

    public void incrementForTopic(String topic, @ServerIdType int server) {
        VectorClock vectorClock = vectorClocksPerTopic.get(topic);
        if (vectorClock == null) {
            throw new IllegalArgumentException("Topic " + topic + " not found");
        }
        vectorClock.increment(server);
    }

    public void selfIncrementForTopic(String topic) {
        VectorClock vectorClock = vectorClocksPerTopic.get(topic);
        if (vectorClock == null) {
            throw new IllegalArgumentException("Topic " + topic + " not found");
        }
        vectorClock.increment(selfIdentifier);
    }
}
