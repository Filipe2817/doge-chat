package com.doge.aggregation.server.neighbour;

import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.zmq.PushEndpoint;

public class Neighbour implements Comparable<Neighbour> {
    private final int id;
    private int age;

    private PushEndpoint pushEndpoint;

    private Logger logger;

    public Neighbour(int id, PushEndpoint pushEndpoint, int age, Logger logger) {
        this.id = id;
        this.age = age;

        this.pushEndpoint = pushEndpoint;

        this.logger = logger;
    }

    public void connect() {
        try {
            pushEndpoint.connectSocket("localhost", this.id);
        } catch (Exception e) {
            logger.error("[PUSH] Failed to connect to neighbour '" + id + "': " + e.getMessage());
        }
    }

    public void disconnect() {
        try {
            this.pushEndpoint.close();
        } catch (Exception e) {
            logger.error("[PUSH] Failed to disconnect from neighbour '" + id + "': " + e.getMessage());
        }
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
            logger.error("[PUSH] Failed to send message to neighbour '" + id + "': " + e.getMessage());
        }
    }
}
