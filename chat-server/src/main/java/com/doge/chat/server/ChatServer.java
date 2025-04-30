package com.doge.chat.server;

import com.doge.chat.server.causal.CausalDeliveryManager;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.handler.*;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.chat.server.socket.zmq.PullEndpoint;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.chat.server.socket.zmq.SubEndpoint;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class ChatServer {
    private volatile boolean running;
    private final int id;
    private final String topic;

    private PullEndpoint pullEndpoint;
    private SubEndpoint subEndpoint;
    private RepEndpoint repEndpoint;
    private PubEndpoint clientPubEndpoint;
    private PubEndpoint chatServerPubEndpoint;

    private VectorClockManager vectorClockManager;
    private CausalDeliveryManager causalDeliveryManager;

    private final Logger logger;

    public ChatServer(
        int id,
        String topic,
        PullEndpoint pullEndpoint,
        SubEndpoint subEndpoint,
        RepEndpoint repEndpoint,
        PubEndpoint clientPubEndpoint,
        PubEndpoint chatServerPubEndpoint,
        VectorClockManager vectorClockManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.topic = topic;

        this.pullEndpoint = pullEndpoint;
        this.repEndpoint = repEndpoint;
        this.subEndpoint = subEndpoint;
        this.clientPubEndpoint = clientPubEndpoint;
        this.chatServerPubEndpoint = chatServerPubEndpoint;
        
        this.logger = logger;

        this.vectorClockManager = vectorClockManager;
        this.causalDeliveryManager = new CausalDeliveryManager(
            this.vectorClockManager,
            this.clientPubEndpoint,
            this.logger
        );
    }

    public int getId() {
        return id;
    }

    public void run() {
        this.running = true;

        Thread pullThread = new Thread(() -> this.runPull(), "Pull-Thread");
        Thread repThread = new Thread(() -> this.runRep(), "Rep-Thread");
        Thread subscriberThread = new Thread(() -> this.runSubscriber(), "Subscriber-Thread");

        try {
            pullThread.start();
            repThread.start();
            subscriberThread.start();

            pullThread.join();
            repThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            pullThread.interrupt();
            repThread.interrupt();
            subscriberThread.interrupt();
        } finally {
           this.stop();
        }
    }

    private void runPull() {
        this.pullEndpoint.on(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(
            this,
            this.logger,
            this.clientPubEndpoint,
            this.chatServerPubEndpoint,
            this.vectorClockManager
        ));

        while (this.running) {
            try {
                pullEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[PULL] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void runRep() {
        this.repEndpoint.on(MessageTypeCase.GETONLINEUSERSMESSAGE, new GetOnlineUsersMessageHandler(
            this.logger,
            this.repEndpoint

            // TODO: Implement a UserManager to handle online users
        ));

        while (this.running) {
            try {
                repEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[REP] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void runSubscriber() {
        this.subEndpoint.on(MessageTypeCase.FORWARDCHATMESSAGE, new ForwardChatMessageHandler(
            this.logger,
            this.causalDeliveryManager
        ));

        this.subEndpoint.subscribe(this.topic);
        logger.info("Chat server is now subscribed to topic: " + this.topic);

        while (this.running) {
            try {
                subEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[SUB] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void stop() {
        this.running = false;

        this.pullEndpoint.close();
        this.subEndpoint.close();
        this.clientPubEndpoint.close();
        this.chatServerPubEndpoint.close();
        
        logger.info("Chat server stopped");
    }
}
