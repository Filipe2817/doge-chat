package com.doge.aggregation.server;

import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.handler.ShuffleMessageHandler;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class AggregationServer {
    private volatile boolean running;
    private final int id;
    private final int l; // how many peer‑entries you send in the request
    private final int i; // how many peer‑entries you take in from the reply

    private PullEndpoint pullEndpoint;

    private NeighbourManager neighbourManager;

    private final Logger logger;

    public AggregationServer(
        int id,
        int l,
        int i,
        PullEndpoint pullEndpoint,
        NeighbourManager neighbourManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;

        this.l = l;
        this.i = i;

        this.pullEndpoint = pullEndpoint;
        this.neighbourManager = neighbourManager;

        this.logger = logger;
    }

    public int getId() {
        return id;
    }

    public void run() {
        this.running = true;

        Thread pullThread = new Thread(() -> this.runPull(), "Pull-Thread");

        try {
            pullThread.start();
            pullThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Aggregation server interrupted: " + e.getMessage());
        }
    }

    private void runPull() {
        this.pullEndpoint.on(MessageTypeCase.SHUFFLEMESSAGE, new ShuffleMessageHandler(
            this,
            this.pullEndpoint,
            this.neighbourManager,
            this.l,
            logger
        ));

        while (this.running) {
            try {
                this.pullEndpoint.receiveOnce();
                // TODO: trigger periodic shuffle here

            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[PULL] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                logger.error("Error in pull thread: " + e.getMessage());
            }
        }
    }
}
