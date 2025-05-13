package com.doge.chat.server.log;

import com.doge.chat.server.causal.VectorClock;
import com.doge.common.proto.ChatMessage;

public record LogMessage(ChatMessage chatMessage, VectorClock vectorClock, int senderId) {}
