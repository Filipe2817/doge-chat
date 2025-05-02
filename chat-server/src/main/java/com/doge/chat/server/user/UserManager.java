package com.doge.chat.server.user;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserManager {
    private Map<String, Set<String>> onlineUsersPerTopic;
    
    public UserManager() {
        this.onlineUsersPerTopic = new HashMap<>();
    }

    public void addUserToTopic(String topic, String userId) {
        this.onlineUsersPerTopic.computeIfAbsent(topic, k -> new HashSet<>()).add(userId);
    }

    public void removeUserFromTopic(String topic, String userId) {
        Set<String> users = this.onlineUsersPerTopic.get(topic);
        if (users != null) {
            users.remove(userId);
            if (users.isEmpty()) {
                this.onlineUsersPerTopic.remove(topic);
            }
        }
    }

    public Set<String> getOnlineUsersForTopic(String topic) {
        return this.onlineUsersPerTopic.getOrDefault(topic, new HashSet<>());
    }
}
