package com.doge.chat.server.user;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.doge.chat.server.causal.ServerIdType;

public class UserManager {
    private @ServerIdType int selfIdentifier;

    // Topic -> ORSet CRDT for online users
    private Map<String, OnlineUsersORSet> onlineUsersPerTopic;
    
    public UserManager(@ServerIdType int selfIdentifier) {
        this.selfIdentifier = selfIdentifier;
        this.onlineUsersPerTopic = new HashMap<>();
    }

    public void addTopic(String topic, List<Integer> servers) {
        OnlineUsersORSet onlineUsersORSet = new OnlineUsersORSet(this.selfIdentifier, servers);
        this.onlineUsersPerTopic.put(topic, onlineUsersORSet);
    }

    public void addUserToTopic(String topic, String userId) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        OnlineUsersORSet onlineUsersORSet = this.onlineUsersPerTopic.get(topic);
        onlineUsersORSet.addUser(userId);
    }

    public void removeUserFromTopic(String topic, String userId) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        OnlineUsersORSet onlineUsers = this.onlineUsersPerTopic.get(topic);
        onlineUsers.removeUser(userId);
    }

    public Set<String> getOnlineUsersForTopic(String topic) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        OnlineUsersORSet onlineUsersORSet = this.onlineUsersPerTopic.get(topic);
        return onlineUsersORSet.getOnlineUsers();
    }

    public DotStore getDotStoreForTopic(String topic) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        OnlineUsersORSet onlineUsersORSet = this.onlineUsersPerTopic.get(topic);
        return onlineUsersORSet.getDotStore();
    }

    public OnlineUsersORSet getOnlineUsersORSetForTopic(String topic) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        return this.onlineUsersPerTopic.get(topic);
    }

    public void updateDotStoreForTopic(String topic, OnlineUsersORSet onlineUsersORSet) {
        if (!this.onlineUsersPerTopic.containsKey(topic)) {
            throw new IllegalArgumentException("Topic does not exist: " + topic);
        }

        OnlineUsersORSet existingOnlineUsersORSet = this.onlineUsersPerTopic.get(topic);
        existingOnlineUsersORSet.join(onlineUsersORSet);
    }
}
