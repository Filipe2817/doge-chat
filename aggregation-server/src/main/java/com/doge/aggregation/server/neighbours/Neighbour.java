package com.doge.aggregation.server.neighbours;

import java.util.Objects;

import com.doge.aggregation.server.Logger;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.socket.zmq.PushEndpoint;

public class Neighbour implements Comparable<Neighbour> {
    private final String address;
    private int age;

    private PushEndpoint pushEndpoint;
    private PullEndpoint pullEndpoint;

    private final Logger logger;

    public Neighbour(String address, PullEndpoint pullEndpoint, PushEndpoint pushEndpoint, int age, Logger logger) {
        this.address = address;
        this.age = age;

        this.pullEndpoint = pullEndpoint;
        this.pushEndpoint = pushEndpoint;

        this.logger = logger;
    }

    public String getAddress() {
        return address;
    }

    public int getAge() {
        return age;
    }

    public void incrementAge() {
        age++;
    }

    public void resetAge() {
        age = 0;
    }

    @Override
    public int compareTo(Neighbour other) {
        return Integer.compare(this.age, other.age);
    }

}