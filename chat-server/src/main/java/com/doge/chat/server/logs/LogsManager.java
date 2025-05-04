package com.doge.chat.server.logs;

import com.doge.chat.server.causal.VectorClock;
import com.doge.common.proto.ForwardChatMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;


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

    public List<ForwardChatMessage> getLastLogsSlice(Integer length) {
        int listLen = logs.size();
        return getLogsSlice(listLen - length, listLen);
    }

    public List<ForwardChatMessage> getLogsSliceFromUser(Integer from,Integer to, String userId) {
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
        List<ForwardChatMessage> logsDup = new ArrayList<>(logs);
        List<ForwardChatMessage> userLogs = logsDup.stream().filter(log -> log.getChatMessage().getClientId().equals(userId)).collect(Collectors.toList());
        return userLogs.subList(from, to);
    }

    public List<ForwardChatMessage> getLastLogsSliceFromUser(Integer length, String userId) {
        int listLen = logs.size();
        return getLogsSliceFromUser(listLen - length, listLen, userId);
    }

    public List<ForwardChatMessage> getLogsFromUser(String userId) {
        List<ForwardChatMessage> logsDup = new ArrayList<>(logs);
        List<ForwardChatMessage> userLogs = logsDup.stream().filter(log -> log.getChatMessage().getClientId().equals(userId)).collect(Collectors.toList());
        return userLogs;
    }

    public List<ForwardChatMessage> getLogs() {
        return logs;
    }
}
