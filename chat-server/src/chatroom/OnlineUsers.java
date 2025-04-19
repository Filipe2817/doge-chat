package chatroom;

import chatroom.util.ChatServerIdentity;
import chatroom.util.VectorClock;
import client.User;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OnlineUsers {

    private VectorClock vectorClock;
    private Map<User,Set<Pair<ChatServerIdentity,Integer>>> users;

    public OnlineUsers(Iterable< ChatServerIdentity> servers) {
        this.vectorClock = new VectorClock(servers);
        this.users = new HashMap<>();
    }

    //Adds a user to the online users list
    public void addUser(User user, ChatServerIdentity self) {
        vectorClock.increment(self);
        users.putIfAbsent(user, new HashSet<>());
        users.computeIfPresent(user, (k, v) -> {
            v.add(Pair.of(self, vectorClock.get(self)));
            return v;
        });
    }

    public void removeUser(User user, ChatServerIdentity self) {
        vectorClock.increment(self);
        users.remove(user);
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public Map<User, Set<Pair<ChatServerIdentity, Integer>>> getUsersMap() {
        return users;
    }

    //Joins two OnlineUsers states together
    public void join(OnlineUsers onlineUsers) {
        Map<User,Set<Pair<ChatServerIdentity,Integer>>> selfUsersStore = this.getUsersMap();
        Map<User,Set<Pair<ChatServerIdentity,Integer>>> otherUsersStore = onlineUsers.getUsersMap();
        VectorClock selfCausalContext = this.getVectorClock();
        VectorClock otherCausalContext = onlineUsers.getVectorClock();

        //Get set of all Users present in either state
        Set<User> usersInEitherState = new HashSet<User>(selfUsersStore.keySet());
        usersInEitherState.addAll(otherUsersStore.keySet());

        //Join the dot stores for each user
        for(User user: usersInEitherState) {
            Set<Pair<ChatServerIdentity,Integer>> selfDotStore = selfUsersStore.getOrDefault(user, new HashSet<>());
            Set<Pair<ChatServerIdentity,Integer>> otherDotStore = otherUsersStore.getOrDefault(user, new HashSet<>());
            Set<Pair<ChatServerIdentity, Integer>> joinedDotStore = joinDotStores(selfDotStore, otherDotStore, selfCausalContext, otherCausalContext);
            //If joined dot store is empty, remove the user from the online users list
            if(joinedDotStore.isEmpty()){
                this.users.remove(user);
            } else {
                //Otherwise, add the user to the online users list with the joined dot store
                this.users.put(user, joinedDotStore);
            }
        }
        //Join the vector clocks
        this.vectorClock.join(otherCausalContext);
    }


    private static Set<Pair<ChatServerIdentity, Integer>> joinDotStores(Set<Pair<ChatServerIdentity, Integer>> selfDotStore,
                                                                        Set<Pair<ChatServerIdentity, Integer>> otherDotStore,
                                                                        VectorClock selfCausalContext, VectorClock otherCausalContext) {
        //Start with dots present in both stores
        Set<Pair<ChatServerIdentity, Integer>> joinedDotStore = new HashSet<>(selfDotStore);
        joinedDotStore.retainAll(otherDotStore);

        //Add dots present just in the self store that are up to date
        Set<Pair<ChatServerIdentity, Integer>> onlySelfDots = new HashSet<>(selfDotStore);
        onlySelfDots.removeAll(joinedDotStore);
        for(Pair<ChatServerIdentity, Integer> dot: onlySelfDots){
            //Check if the dot is made after the other causal context
            if(dot.getRight() >= otherCausalContext.get(dot.getLeft())){
                joinedDotStore.add(dot);
            }
        }
        //Add dots present just in the other store that are up to date
        Set<Pair<ChatServerIdentity, Integer>> onlyOtherDots = new HashSet<>(otherDotStore);
        onlyOtherDots.removeAll(joinedDotStore);
        for(Pair<ChatServerIdentity, Integer> dot: onlyOtherDots){
            //Check if the dot is made after the other causal context
            if(dot.getRight() >= selfCausalContext.get(dot.getLeft())){
                joinedDotStore.add(dot);
            }
        }
        return joinedDotStore;
    }
}
