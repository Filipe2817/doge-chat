package com.doge.aggregation.server.gossip;

import java.util.List;

public record AggregationState(
    String topic,
    boolean startedByUs,
    boolean finished,
    int t,
    List<ChatServerState> chatServers
) {}
