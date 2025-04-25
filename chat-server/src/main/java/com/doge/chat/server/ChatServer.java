package com.doge.chat.server;

import com.doge.chat.server.causal.CausalDeliveryManager;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.handler.ChatMessageHandler;
import com.doge.chat.server.handler.ForwardChatMessageHandler;
import com.doge.chat.server.socket.PubEndpoint;
import com.doge.chat.server.socket.PullEndpoint;
import com.doge.chat.server.socket.SubEndpoint;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class ChatServer {
    private volatile boolean running;
    private final int id;
    private final String topic;

    private PullEndpoint pullEndpoint;
    private SubEndpoint subEndpoint;
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
        PubEndpoint clientPubEndpoint,
        PubEndpoint chatServerPubEndpoint,
        VectorClockManager vectorClockManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.topic = topic;

        this.pullEndpoint = pullEndpoint;
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

        Thread pullerThread = new Thread(() -> this.runPuller(), "Puller-Thread");
        Thread subscriberThread = new Thread(() -> this.runSubscriber(), "Subscriber-Thread");

        try {
            pullerThread.start();
            subscriberThread.start();

            pullerThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            pullerThread.interrupt();
            subscriberThread.interrupt();
        } finally {
           this.stop();
        }
    }

    private void runPuller() {
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
                logger.debug("Error while receiving message: " + e.getMessage());
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
                logger.debug("Error while receiving message: " + e.getMessage());
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
