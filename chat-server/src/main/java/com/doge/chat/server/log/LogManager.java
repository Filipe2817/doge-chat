package com.doge.chat.server.log;

import com.doge.chat.server.causal.VectorClock;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogManager {
    private Map<String, List<LogMessage>> logsPerTopic;

    public LogManager() {
        this.logsPerTopic = new HashMap<>();
    }

    public void addLog(ForwardChatMessage forward) {
        ChatMessage chatMessage = forward.getChatMessage();
        String topic = chatMessage.getTopic();

        List<LogMessage> logs = logsPerTopic.computeIfAbsent(
            topic, k -> new ArrayList<>()
        );

        LogMessage log = new LogMessage(
            chatMessage,
            new VectorClock(forward.getVectorClockMap()),
            forward.getSenderId()
        );

        int index = Collections.binarySearch(
            logs,
            log, 
            LogMessageComparator.compare
        );

        if (index < 0) {
            index = -(index + 1);
            logs.add(index, log);
        }
    }

    public List<LogMessage> snapshot(String topic) {
        List<LogMessage> logs = logsPerTopic.get(topic);
        if (logs == null) {
            return Collections.emptyList();
        }

        return new ArrayList<>(logs);
    }
}
