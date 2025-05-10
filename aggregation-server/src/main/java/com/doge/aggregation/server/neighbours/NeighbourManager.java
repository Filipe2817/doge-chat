package com.doge.aggregation.server.neighbours;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.doge.aggregation.server.Logger;

public class NeighbourManager {
    private final int cacheSize;
    private final Map<Integer, Neighbour> cache;
    private final Random random = ThreadLocalRandom.current();

    public NeighbourManager(int cacheSize) {
        this.cacheSize = cacheSize;
        this.cache = new HashMap<>(cacheSize);
    }

    public void ageAll() {
        for (Neighbour n : cache.values()) {
            n.incrementAge();
        }
    }

    public Neighbour get(Integer id) {
        return cache.get(id);
    }

    public Neighbour getOldest() {
        List<Neighbour> neighbours = new ArrayList<>(cache.values());
        Collections.sort(neighbours);
        return neighbours.get(0);
    }

    public void addNeighbour(Neighbour n) {
        if (cache.size() >= cacheSize) {
            Neighbour oldest = getOldest();
            if (oldest != null) {
                cache.remove(oldest.getId());
            }
        }
        cache.put(n.getId(), n);
    }

    public Neighbour remove(Integer id) {
        return cache.remove(id);
    }

    public List<Neighbour> pickRandom(int k, Neighbour exclude) {
        List<Neighbour> candidates = new ArrayList<>();
        for (Neighbour n : cache.values()) {
            if (!n.equals(exclude)) {
                candidates.add(n);
            }
        }
        Collections.shuffle(candidates, this.random);
        int num = Math.min(k, candidates.size());
        return new ArrayList<>(candidates.subList(0, num));
    }

    public int size() {
        return cache.size();
    }
}
