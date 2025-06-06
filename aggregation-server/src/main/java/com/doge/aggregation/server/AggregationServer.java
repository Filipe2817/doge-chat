package com.doge.aggregation.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZContext;

import com.doge.aggregation.server.handler.ShuffleMessageHandler;
import com.doge.aggregation.server.neighbour.NeighbourManager;
import com.doge.aggregation.server.cyclon.CyclonManager;
import com.doge.aggregation.server.socket.tcp.DhtClient;
import com.doge.aggregation.server.gossip.GossipManager;
import com.doge.aggregation.server.handler.AggregationCurrentStateMessageHandler;
import com.doge.aggregation.server.handler.AggregationStartMessageHandler;
import com.doge.aggregation.server.handler.RandomWalkMessageHandler;
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
    private DhtClient dhtClient;

    private NeighbourManager neighbourManager;
    private CyclonManager cyclonManager;
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
        DhtClient dhtClient,
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
        this.dhtClient = dhtClient;

        this.logger = logger;

        this.neighbourManager = neighbourManager;
        this.cyclonManager = new CyclonManager(
            this.l,
            this,
            this.context,
            this.neighbourManager,
            this.logger
        );
        this.gossipManager = new GossipManager(this.neighbourManager, this.logger);
    }

    public int getId() {
        return id;
    }

    public void run() {
        this.running = true;

        List<Thread> threads = new ArrayList<>();

        Thread pullThread = Thread.ofVirtual()
            .name("Pull-Thread")
            .start(this::runPull);

        threads.add(pullThread);

        Thread repThread = Thread.ofVirtual()
            .name("Rep-Thread")
            .start(this::runRep);

        threads.add(repThread);

        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }
        } finally {
            this.stop();
        }
    }

    private void runPull() {
        ShuffleMessageHandler shuffleMessageHandler = new ShuffleMessageHandler(
            cyclonManager,
            this.logger
        );

        RandomWalkMessageHandler randomWalkMessageHandler = new RandomWalkMessageHandler(
            cyclonManager,
            this.logger
        );

        this.pullEndpoint.on(MessageTypeCase.SHUFFLEMESSAGE, shuffleMessageHandler);
        this.pullEndpoint.on(MessageTypeCase.RANDOMWALKMESSAGE, randomWalkMessageHandler);
        this.pullEndpoint.on(MessageTypeCase.AGGREGATIONCURRENTSTATEMESSAGE, new AggregationCurrentStateMessageHandler(
            this.context,
            this.repEndpoint,
            this.reqEndpoint,
            this.dhtClient,
            this.gossipManager,
            this.logger
        ));

        cyclonManager.triggerRandomWalk();

        // Schedule periodic shuffle trigger every 10 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cyclonManager.triggerShuffle();
            } catch(Exception e) {
                logger.error("Error triggering shuffle: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS);

        while (this.running) {
            try {
                this.pullEndpoint.receiveOnce();

                logger.info("Neighbours cache");
                System.out.println(neighbourManager.toString());
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
