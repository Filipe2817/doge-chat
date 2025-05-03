package com.doge.chat.server.user;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.doge.chat.server.causal.ServerIdType;
import com.doge.chat.server.causal.VectorClock;

public class OnlineUsersORSet {
    private @ServerIdType int selfIdentifier;

    private VectorClock vectorClock;

    // Users identifiers are the keys of the store
    private DotStore dotStore;

    public OnlineUsersORSet(@ServerIdType int selfIdentifier, List<Integer> servers) {
        this.selfIdentifier = selfIdentifier;
        
        this.vectorClock = new VectorClock(servers);
        
        this.dotStore = new DotStore();
    }

    public OnlineUsersORSet(DotStore dotStore, VectorClock vectorClock) {
        this.vectorClock = vectorClock;

        this.dotStore = dotStore; 
    }

    public DotStore getDotStore() {
        return dotStore;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public Set<String> getOnlineUsers() {
        return dotStore.keySet();
    }

    public void addUser(@UserIdType String user) {
        vectorClock.increment(this.selfIdentifier);

        dotStore.putIfAbsent(user, new DotSet());
        dotStore.computeIfPresent(user, (k, v) -> {
            Pair<Integer, Integer> dot = Pair.of(this.selfIdentifier, vectorClock.get(this.selfIdentifier));
            v.add(dot);
            return v;
        });
    }

    public void removeUser(@UserIdType String user) {
        vectorClock.increment(this.selfIdentifier);
        dotStore.remove(user);
    }

    public void join(OnlineUsersORSet onlineUsers) {
        DotStore selfDotStore = this.getDotStore();
        DotStore otherDotStore = onlineUsers.getDotStore();

        VectorClock selfCausalContext = this.getVectorClock();
        VectorClock otherCausalContext = onlineUsers.getVectorClock();

        // Get set of all users present in either state
        Set<String> usersInEitherState = new HashSet<String>(selfDotStore.keySet());
        usersInEitherState.addAll(otherDotStore.keySet());

        // Pointwise join the dot stores
        for (String user : usersInEitherState) {
            DotSet selfDotSet = selfDotStore.getOrDefault(user, new DotSet());
            DotSet otherDotSet = otherDotStore.getOrDefault(user, new DotSet());

            DotSet joinedDotSet = OnlineUsersORSet.joinDotSets(
                selfDotSet,
                otherDotSet,
                selfCausalContext, 
                otherCausalContext
            );

            // If joined dot store is empty, remove the user from the online users list
            if (joinedDotSet.isEmpty()) {
                this.dotStore.remove(user);
            } else {
                // Otherwise, add the user to the online users list with the joined dot store
                this.dotStore.put(user, joinedDotSet);
            }
        }

        // Join the vector clocks
        this.vectorClock.join(otherCausalContext);
    }

    private static DotSet joinDotSets(
        DotSet selfDotSet,
        DotSet otherDotSet,
        VectorClock selfCausalContext, 
        VectorClock otherCausalContext
    ) {
        // Start with dots present in both stores
        DotSet joinedDotSet = new DotSet(selfDotSet);
        joinedDotSet.retainAll(otherDotSet);

        // Add dots present just in the self store that are up to date
        DotSet onlySelfDotSet = new DotSet(selfDotSet);
        onlySelfDotSet.removeAll(joinedDotSet);

        for (Pair<Integer, Integer> dot : onlySelfDotSet) {
            // Check if the dot is made after the other causal context
            if (dot.getRight() >= otherCausalContext.get(dot.getLeft())) {
                joinedDotSet.add(dot);
            }
        }

        // Add dots present just in the other store that are up to date
        DotSet onlyOtherDotSet = new DotSet(otherDotSet);
        onlyOtherDotSet.removeAll(joinedDotSet);
        
        for (Pair<Integer, Integer> dot : onlyOtherDotSet) {
            // Check if the dot is made after the other causal context
            if (dot.getRight() >= selfCausalContext.get(dot.getLeft())) {
                joinedDotSet.add(dot);
            }
        }

        return joinedDotSet;
    }
}
