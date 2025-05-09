package com.doge.aggregation.server.neighbours;

import java.util.Objects;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.common.proto.MessageWrapper;


public class Neighbour implements Comparable<Neighbour> {
    private final int id;
    private int age;

    private PushEndpoint pushEndpoint;
    private PullEndpoint pullEndpoint;

    private final Logger logger;

    public Neighbour(int id, PullEndpoint pullEndpoint, PushEndpoint pushEndpoint, int age, Logger logger) {
        this.id = id;
        this.age = age;

        this.pullEndpoint = pullEndpoint;
        this.pushEndpoint = pushEndpoint;

        this.logger = logger;
    }

    public Integer getId() {
        return this.id;
    }

    public int getAge() {
        return this.age;
    }

    public void incrementAge() {
        this.age++;
    }

    public void resetAge() {
        this.age = 0;
    }

    @Override
    public int compareTo(Neighbour other) {
        return Integer.compare(this.age, other.age);
    }

    public void sendMessage(MessageWrapper message) {
        try {
            this.pushEndpoint.send(message);
        } catch (Exception e) {
            logger.error("Failed to send message to neighbour " + id + ": " + e.getMessage());
        }
    }
}