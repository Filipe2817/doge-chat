package com.doge.aggregation.server;

import com.doge.aggregation.server.socket.zmq.PullEndpoint;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZContext;

import com.doge.aggregation.server.handler.ShuffleMessageHandler;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.common.Logger;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class AggregationServer {
    private volatile boolean running;
    private final int id;
    private final int l;

    private ZContext context;
    private PullEndpoint pullEndpoint;

    private NeighbourManager neighbourManager;
    private Logger logger;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public AggregationServer(
        int id,
        int l,
        ZContext context,
        PullEndpoint pullEndpoint,
        NeighbourManager neighbourManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.l = l;

        this.context = context;
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
        } finally {
            this.stop();
        }
    }

    private void runPull() {
        ShuffleMessageHandler shuffleMessageHandler = new ShuffleMessageHandler(
            this.l,
            this,
            this.context,
            this.neighbourManager,
            this.logger
        );

        this.pullEndpoint.on(MessageTypeCase.SHUFFLEMESSAGE, shuffleMessageHandler);

        // Schedule periodic shuffle trigger every 10 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                shuffleMessageHandler.triggerShuffle();
            } catch(Exception e) {
                logger.error("Error triggering shuffle: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS);

        while (this.running) {
            try {
                this.pullEndpoint.receiveOnce();

                logger.debug("Neighbours Cache");
                System.out.println(this.neighbourManager);
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[PULL] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private void stop() {
        this.running = false;
        
        this.scheduler.shutdown();
        try {
            if (!this.scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                this.scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Error shutting down scheduler: " + e.getMessage());
            return;
        }

        logger.info("Aggregation server stopped");
    }
}
