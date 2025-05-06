package com.doge.aggregation.server.neighbours;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private final List<Neighbour> cache;
    private final Random random = ThreadLocalRandom.current();

    private final PullEndpoint pullEndpoint;

    private final Logger logger;
    private final ZContext context;

    public NeighbourManager(int cacheSize, Logger logger, PullEndpoint pullEndpoint, ZContext context) {
        this.cacheSize = cacheSize;
        this.cache = new ArrayList<>(cacheSize);

        this.pullEndpoint = pullEndpoint;

        this.logger = logger;
        this.context = context;
    }

    public void ageAll() {
        for (Neighbour n : cache) {
            n.incrementAge();
        }
    }

    public void addNeighbour(String port) {
        try {
            String address = "tcp://localhost:" + port;
            PushEndpoint pushEndpoint = new PushEndpoint(context);
            pushEndpoint.connectSocket("localhost", Integer.parseInt(port));
            Neighbour n = new Neighbour(address, this.pullEndpoint, pushEndpoint, 0, logger);
            cache.add(n);

            if (cache.size() > cacheSize) {
                Collections.sort(cache);
                cache.remove(0);
            }
        } catch (Exception e) {
            logger.error("Failed to add neighbour: " + e.getMessage());
        }
    }


    public void remove(String address) {
        cache.removeIf(n -> n.getAddress().equals(address));
    }

    public List<Neighbour> pickRandom(int k) {
        List<Neighbour> candidates = new ArrayList<>(cache);
        Collections.shuffle(candidates, this.random);
        return candidates.subList(0, Math.min(k, candidates.size()));
    }
}
