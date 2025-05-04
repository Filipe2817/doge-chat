package com.doge.chat.server.logs;

import com.doge.chat.server.causal.VectorClock;
import com.doge.common.proto.ForwardChatMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class LogsManager {
    //Compare vector clocks, then compare sender
    private final static Comparator<ForwardChatMessage> messageComparator = Comparator.comparing(ForwardChatMessage::getVectorClockMap, (a,b) ->{
        VectorClock aClock = new VectorClock(a);
        VectorClock bClock = new VectorClock(b);
        return aClock.compare(bClock);
    }).thenComparingInt(ForwardChatMessage::getSenderId);

    private List<ForwardChatMessage> logs;


    public LogsManager() {
        this.logs = new ArrayList<ForwardChatMessage>();
    }

    public void addLog(ForwardChatMessage log) {
        int index = Collections.binarySearch(logs, log, messageComparator);
        if(index < 0) {
            index = -(index + 1);
        }
        else {
            //If the log is already in the list, we don't need to add it again
            return;
        }
        logs.add(index, log);
    }

    //TODO?: Make the slice selectable by timestamp?
    public List<ForwardChatMessage> getLogsSlice(Integer from, Integer to) {
        if (from == null){
            from = 0;
        }
        if(to == null){
            to = logs.size();
        }
        if (from < 0 || to > logs.size() || from > to) {
            System.out.println("Invalid range");
            return Collections.emptyList();
        }
        return logs.subList(from, to);
    }

    public List<ForwardChatMessage> getLogs() {
        return logs;
    }
}
