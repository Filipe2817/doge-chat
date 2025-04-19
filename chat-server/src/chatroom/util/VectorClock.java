package chatroom.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VectorClock {

    private Map<ChatServerIdentity,Integer> vectorClock;

    public VectorClock(Iterable<ChatServerIdentity> servers) {
        this.vectorClock = new HashMap<>();
        for (ChatServerIdentity server : servers) {
            vectorClock.put(server, 0);
        }
    }

    public void increment(ChatServerIdentity server) {
        vectorClock.compute(server, (k, v) -> v == null ? 1 : v + 1);
    }

    public void update(ChatServerIdentity server, Integer value) {
        vectorClock.put(server, value);
    }

    public Integer get(ChatServerIdentity server) {
        return vectorClock.getOrDefault(server, 0);
    }

    //Compares this vector clock to another vector clock
    //Returns -1 if this vector clock is before the other, 1 if this vector clock is after the other, and 0 if they are concurrent
    public int compare(VectorClock other){
        //Will be true in the end of the comparison if the clock values are always less or equal to the other
        boolean beforeAll = true;
        //Will be true in the end of the comparison if the clock values are always greater or equal to the other
        boolean afterAll = true;
        //If neither are true, they are concurrent
        for (Map.Entry<ChatServerIdentity, Integer> entry : vectorClock.entrySet()) {
            ChatServerIdentity sender = entry.getKey();
            int thisValue = entry.getValue();
            int otherValue = other.vectorClock.get(sender);
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

    public void join(VectorClock other) {
        Set<ChatServerIdentity> servers = new HashSet<>(vectorClock.keySet());
        servers.addAll(other.vectorClock.keySet());
        for (ChatServerIdentity server : servers) {
            int thisValue = this.get(server);
            int otherValue = other.get(server);
            this.update(server, Math.max(thisValue, otherValue));
        }
    }
}
