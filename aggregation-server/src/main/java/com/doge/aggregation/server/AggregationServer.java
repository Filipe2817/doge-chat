package com.doge.aggregation.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZContext;

import com.doge.aggregation.server.handler.ShuffleMessageHandler;
import com.doge.aggregation.server.neighbour.NeighbourManager;
import com.doge.aggregation.server.gossip.GossipManager;
import com.doge.aggregation.server.handler.AggregationCurrentStateMessageHandler;
import com.doge.aggregation.server.handler.AggregationStartMessageHandler;
import com.doge.common.Logger;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.doge.common.socket.zmq.PullEndpoint;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;

public class AggregationServer {
    private volatile boolean running;
    private final int id;
    private final int l;

    private ZContext context;
    private PullEndpoint pullEndpoint;
    private RepEndpoint repEndpoint;
    private ReqEndpoint reqEndpoint;

    private NeighbourManager neighbourManager;
    private GossipManager gossipManager;
    private Logger logger;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public AggregationServer(
        int id,
        int l,
        ZContext context,
        PullEndpoint pullEndpoint,
        RepEndpoint repEndpoint,
        ReqEndpoint reqEndpoint,
        NeighbourManager neighbourManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.l = l;

        this.context = context;
        this.pullEndpoint = pullEndpoint;
        this.repEndpoint = repEndpoint;
        this.reqEndpoint = reqEndpoint;

        this.neighbourManager = neighbourManager;
        this.logger = logger;
        this.gossipManager = new GossipManager(this.neighbourManager, this.logger);
    }

    public int getId() {
        return id;
    }

    public void run() {
        this.running = true;

        Thread pullThread = new Thread(() -> this.runPull(), "Pull-Thread");
        Thread repThread = new Thread(() -> this.runRep(), "Rep-Thread");

        try {
            pullThread.start();
            repThread.start();

            pullThread.join();
            repThread.join();
        } catch (InterruptedException e) {
            pullThread.interrupt();
            repThread.interrupt();
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
        this.pullEndpoint.on(MessageTypeCase.AGGREGATIONCURRENTSTATEMESSAGE, new AggregationCurrentStateMessageHandler(
            this.repEndpoint,
            this.reqEndpoint,
            this.gossipManager,
            this.logger
        ));

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
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[PULL] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private void runRep() {
        this.repEndpoint.on(MessageTypeCase.AGGREGATIONSTARTMESSAGE, new AggregationStartMessageHandler(
            this.reqEndpoint,
            this.gossipManager,
            this.logger
        ));

        while (this.running) {
            try {
                this.repEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[REP] Error while receiving message: " + e.getMessage());
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

        this.pullEndpoint.close();
        this.repEndpoint.close();
        this.reqEndpoint.close();

        logger.info("Aggregation server stopped");
    }
}
