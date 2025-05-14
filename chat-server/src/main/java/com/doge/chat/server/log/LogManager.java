package com.doge.chat.server.log;

import com.doge.chat.server.causal.VectorClock;
import com.doge.common.proto.ForwardChatMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LogManager {
    private List<LogMessage> logs;

    public LogManager() {
        this.logs = new ArrayList<>();
    }

    public void addLog(ForwardChatMessage forward) {
        LogMessage log = new LogMessage(
            forward.getChatMessage(),
            new VectorClock(forward.getVectorClockMap()),
            forward.getSenderId()
        );

        int index = Collections.binarySearch(
            this.logs, 
            log, 
            LogMessageComparator.compare
        );

        if (index < 0) {
            index = -(index + 1);
        } else {
            // If the log is already in the list, we don't need to add it again
            return;
        }

        this.logs.add(index, log);
    }

    public List<LogMessage> snapshot() {
        return new ArrayList<>(this.logs);
    }
}
