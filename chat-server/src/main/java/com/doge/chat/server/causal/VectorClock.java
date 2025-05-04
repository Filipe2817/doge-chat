package com.doge.chat.server.causal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VectorClock {
    // TODO: Should this be a ConcurrentHashMap?
    private Map<Integer, Integer> data;

    public VectorClock(List<Integer> servers) {
        this.data = new HashMap<>();
        for (int server : servers) {
            this.data.put(server, 0);
        }
    }

    public VectorClock(Map<Integer, Integer> data) {
        this.data = new HashMap<>(data);
    }

    public Map<Integer, Integer> asData() {
        return new HashMap<>(data);
    }

    public int get(@ServerIdType int server) {
        if (!this.data.containsKey(server)) {
            throw new IllegalArgumentException("Server " + server + " not found in vector clock");
        }

        return this.data.get(server);
    }

    public void increment(@ServerIdType int server) {
        if (!this.data.containsKey(server)) {
            throw new IllegalArgumentException("Server " + server + " not found in vector clock");
        }

        int current = this.data.get(server);
        this.data.put(server, current + 1);
    }

    //Compares this vector clock to another vector clock
    //Returns -1 if this vector clock is before the other, 1 if this vector clock is after the other, and 0 if they are concurrent
    public int compare(VectorClock other){
        //Will be true in the end of the comparison if the clock values are always less or equal to the other
        boolean beforeAll = true;
        //Will be true in the end of the comparison if the clock values are always greater or equal to the other
        boolean afterAll = true;
        //If neither are true, they are concurrent
        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            Integer sender = entry.getKey();
            int thisValue = entry.getValue();
            int otherValue = other.data.get(sender);
            if (thisValue < otherValue) {
                afterAll = false; //If this vector clock has some value smaller than the other, it means it cannot be after it
            } else if (thisValue > otherValue) {
                beforeAll = false; //If this vector clock has some value greater than the other, it means it cannot be before it
            }
        }
        if (beforeAll && afterAll) {
            return 0; //They are the same
        } else if (beforeAll) {
            return -1; //This vector clock is before the other
        } else if (afterAll) {
            return 1; //This vector clock is after the other
        } else {
            return 0; //They are concurrent
        }
    }

    public boolean isCausalDeliverable(VectorClock other, @ServerIdType int server) {
        // For all k != j (where j is the server of incoming message)
        // if Vm[k] (incoming message) <= V[k] then we can deliver the message
        // V[j] + 1 should also be equal to Vm[j]
        //
        // V[j] + 1 should also be equal to Vm[j]

        if (!other.data.containsKey(server)) {
            throw new IllegalArgumentException("Server " + server + " not found in vector clock");
        }

        int otherValue = other.data.get(server);
        int selfValue = this.data.getOrDefault(server, 0);

        if (otherValue != selfValue + 1) {
            return false;
        }

        for (Map.Entry<Integer, Integer> entry : other.data.entrySet()) {
            int serverId = entry.getKey();
            int otherServerValue = entry.getValue();

            if (serverId == server) {
                continue;
            }

            int selfServerValue = this.data.getOrDefault(serverId, 0);
            if (otherServerValue > selfServerValue) {
                return false;
            }
        }

        return true;
    }

    public boolean isConcurrent(VectorClock other) {
        // Two vector clocks are said to be concurrent iff
        // neither self <= other nor other <= self
        // 
        // That is, the two clocks are incomparable
        boolean selfLeqOther = true;
        boolean otherLeqSelf = true;

        Set<Integer> all = new HashSet<>(this.data.keySet());
        all.addAll(other.data.keySet());

        for (Integer server : all) {
            int selfValue = this.data.getOrDefault(server, 0);
            int otherValue = other.data.getOrDefault(server, 0);

            if (selfValue > otherValue) {
                otherLeqSelf = false;
            }

            if (otherValue > selfValue) {
                selfLeqOther = false;
            }

            if (!selfLeqOther && !otherLeqSelf) {
                return true;
            }
        }

        return false;
    }

    public void join(VectorClock other) {
        Set<Integer> all = new HashSet<>(this.data.keySet());
        all.addAll(other.data.keySet());

        // Pointwise maximum of both vector clocks
        for (Integer server : all) {
            int selfValue = this.data.getOrDefault(server, 0);
            int otherValue = other.data.getOrDefault(server, 0);

            int max = Math.max(selfValue, otherValue);
            this.data.put(server, max);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");

        Iterator<Map.Entry<Integer, Integer>> it = this.data.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Integer> entry = it.next();
            sb.append(entry.getKey()).append(':').append(entry.getValue());

            if (it.hasNext()) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }
}
