package com.doge.chat.server.user;

import java.util.HashMap;

// DotStore
// Map from @UserIdType to DotSet
// Each Dot = Pair<@ServerIdType, Clock>
public class DotStore extends HashMap<String, DotSet> {
    public DotStore() {
        super();
    }
}
 