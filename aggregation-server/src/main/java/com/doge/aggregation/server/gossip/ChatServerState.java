package com.doge.aggregation.server.gossip;

public record ChatServerState(int id, int userCount, int topicCount) implements Comparable<ChatServerState> {
    @Override
    public int compareTo(ChatServerState other) {
        int cmp = Integer.compare(this.userCount, other.userCount);
        if (cmp != 0) return cmp;

        cmp = Integer.compare(this.topicCount, other.topicCount);
        if (cmp != 0) return cmp;

        return Integer.compare(this.id, other.id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(id);
        sb.append(", u=");
        sb.append(userCount);
        sb.append(", t=");
        sb.append(topicCount);
        sb.append(")");
        return sb.toString();
    }
}
