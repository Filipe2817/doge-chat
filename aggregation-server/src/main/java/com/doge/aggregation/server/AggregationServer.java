package com.doge.aggregation.server;

import com.doge.aggregation.server.socket.zmq.PullEndpoint;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZContext;

import com.doge.aggregation.server.handler.ShuffleMessageHandler;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class AggregationServer {
    private volatile boolean running;
    private final int id;
    private final int l; // how many peer‑entries you send in the request

    private PullEndpoint pullEndpoint;

    private NeighbourManager neighbourManager;

    private final ZContext context;
    private final Logger logger;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public AggregationServer(
        int id,
        int l,
        NeighbourManager neighbourManager,
        PullEndpoint pullEndpoint,
        ZContext context,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.l = l;

        this.pullEndpoint = pullEndpoint;

        this.neighbourManager = neighbourManager;

        this.context = context;
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
        ShuffleMessageHandler shuffleMessageHandler = new ShuffleMessageHandler(
            this,
            this.neighbourManager,
            this.l,
            this.pullEndpoint,
            this.context,
            logger
        );

        this.pullEndpoint.on(MessageTypeCase.SHUFFLEMESSAGE, shuffleMessageHandler);

        // Schedule periodic shuffle trigger every 30 seconds (for example)
        scheduler.scheduleAtFixedRate(() -> {
            try {
                shuffleMessageHandler.triggerShuffle();
            } catch(Exception e) {
                logger.error("Error triggering shuffle: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS); // initial delay 10s, then every 30s


        while (this.running) {
            try {
                this.pullEndpoint.receiveOnce();
                logger.debug(neighbourManager.toString());
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[PULL] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                logger.error("Error in pull thread: " + e.getMessage());
            }
        }
    }
}
