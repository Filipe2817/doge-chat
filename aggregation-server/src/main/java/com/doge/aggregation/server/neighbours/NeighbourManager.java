package com.doge.aggregation.server.neighbours;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.zeromq.ZContext;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;

public class NeighbourManager {
    private final int cacheSize;
    private final Map<Integer, Neighbour> cache;
    private final Random random = ThreadLocalRandom.current();

    private final PullEndpoint pullEndpoint;
    private final Logger logger;
    private final ZContext context;

    public NeighbourManager(int cacheSize, Logger logger, PullEndpoint pullEndpoint, ZContext context) {
        this.cacheSize = cacheSize;
        this.cache = new HashMap<>(cacheSize);
        this.pullEndpoint = pullEndpoint;
        this.logger = logger;
        this.context = context;
    }

    public void ageAll() {
        for (Neighbour n : cache.values()) {
            n.incrementAge();
        }
    }

    public Neighbour get(Integer id) {
        return cache.get(id);
    }

    public void addNeighbour(Integer id, int age) {
        try {
            PushEndpoint pushEndpoint = new PushEndpoint(context);
            pushEndpoint.connectSocket("localhost", id);
            Neighbour n = new Neighbour(id, this.pullEndpoint, pushEndpoint, age, logger);
            cache.put(id, n);

            if (cache.size() > cacheSize) {
                List<Neighbour> neighbours = new ArrayList<>(cache.values());
                Collections.sort(neighbours);
                Neighbour toRemove = neighbours.get(0);
                cache.remove(toRemove.getId());
            }
        } catch (Exception e) {
            logger.error("Failed to add neighbour: " + e.getMessage());
        }
    }

    public void addNeighbour(Integer id) {
        addNeighbour(id, 0);
    }

    public void remove(Integer id) {
        cache.remove(id);
    }

    public List<Neighbour> pickRandom(int k) {
        List<Neighbour> candidates = new ArrayList<>(cache.values());
        Collections.shuffle(candidates, this.random);
        return candidates.subList(0, Math.min(k, candidates.size()));
    }
}
