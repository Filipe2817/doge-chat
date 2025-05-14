package com.doge.chat.server.log;

import java.util.Comparator;

public class LogMessageComparator {
    public static final Comparator<LogMessage> compare = Comparator.comparing(
        LogMessage::vectorClock, (a, b) -> {
            return a.compare(b);
        }).thenComparingInt(LogMessage::senderId);
}
