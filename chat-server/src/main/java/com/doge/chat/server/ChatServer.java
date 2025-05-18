package com.doge.chat.server;

import com.doge.chat.server.causal.CausalDeliveryManager;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.handler.AnnounceMessageHandler;
import com.doge.chat.server.handler.ChatMessageHandler;
import com.doge.chat.server.handler.ExitMessageHandler;
import com.doge.chat.server.handler.ForwardChatMessageHandler;
import com.doge.chat.server.handler.ForwardUserOnlineMessageHandler;
import com.doge.chat.server.handler.GetChatServerStateMessageHandler;
import com.doge.chat.server.handler.GetOnlineUsersMessageHandler;
import com.doge.chat.server.handler.NotifyNewTopicMessageHandler;
import com.doge.chat.server.log.LogManager;
import com.doge.chat.server.socket.reactive.ReactiveGrpcEndpoint;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.doge.common.socket.zmq.PubEndpoint;
import com.doge.common.socket.zmq.PullEndpoint;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.SubEndpoint;

public class ChatServer {
    private volatile boolean running;
    private final int id;
    private int topicsBeingServed;

    private PullEndpoint pullEndpoint;
    private SubEndpoint subEndpoint;
    private RepEndpoint repEndpoint;
    private PubEndpoint clientPubEndpoint;
    private PubEndpoint chatServerPubEndpoint;
    private ReactiveGrpcEndpoint reactiveEndpoint;

    private VectorClockManager vectorClockManager;
    private LogManager logManager;
    private CausalDeliveryManager causalDeliveryManager;
    private UserManager userManager;

    private final Logger logger;

    public ChatServer(
        int id,
        PullEndpoint pullEndpoint,
        SubEndpoint subEndpoint,
        RepEndpoint repEndpoint,
        PubEndpoint clientPubEndpoint,
        PubEndpoint chatServerPubEndpoint,
        ReactiveGrpcEndpoint reactiveEndpoint,
        LogManager logManager,
        VectorClockManager vectorClockManager,
        UserManager userManager,
        Logger logger
    ) {
        this.running = false;
        this.id = id;
        this.topicsBeingServed = 0;

        this.pullEndpoint = pullEndpoint;
        this.repEndpoint = repEndpoint;
        this.subEndpoint = subEndpoint;
        this.clientPubEndpoint = clientPubEndpoint;
        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.reactiveEndpoint = reactiveEndpoint;

        this.logManager = logManager;
        this.vectorClockManager = vectorClockManager;
        this.causalDeliveryManager = new CausalDeliveryManager(
            this.vectorClockManager,
            this.logManager,
            this.clientPubEndpoint,
            logger
        );
        this.userManager = userManager;
        
        this.logger = logger;
    }

    public int getId() {
        return id;
    }

    public int getTopicsBeingServed() {
        return topicsBeingServed;
    }

    public void incrementTopicsBeingServed() {
        this.topicsBeingServed++;
    }

    public void run() {
        this.running = true;

        Thread pullThread = new Thread(() -> this.runPull(), "Pull-Thread");
        Thread repThread = new Thread(() -> this.runRep(), "Rep-Thread");
        Thread subscriberThread = new Thread(() -> this.runSub(), "Sub-Thread");
        Thread reactiveThread = new Thread(() -> this.runReactive(), "Reactive-Thread");

        try {
            pullThread.start();
            repThread.start();
            subscriberThread.start();
            reactiveThread.start();

            pullThread.join();
            repThread.join();
            subscriberThread.join();
            reactiveThread.join();
        } catch (InterruptedException e) {
            pullThread.interrupt();
            repThread.interrupt();
            subscriberThread.interrupt();
            reactiveThread.interrupt();
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
            this.vectorClockManager,
            this.logManager
        ));

        this.pullEndpoint.on(MessageTypeCase.EXITMESSAGE, new ExitMessageHandler(
            this.logger, 
            this.chatServerPubEndpoint, 
            this.userManager
        )); 

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
        this.repEndpoint.on(MessageTypeCase.GETONLINEUSERSMESSAGE, new GetOnlineUsersMessageHandler(
            this.logger, 
            this.repEndpoint, 
            this.userManager
        ));
        
        this.repEndpoint.on(MessageTypeCase.ANNOUNCEMESSAGE, new AnnounceMessageHandler(
            this.logger,
            this.chatServerPubEndpoint,
            this.repEndpoint,
            this.userManager
        ));

        this.repEndpoint.on(MessageTypeCase.GETCHATSERVERSTATEMESSAGE, new GetChatServerStateMessageHandler(
            this,
            this.repEndpoint,
            this.userManager
        ));

        this.repEndpoint.on(MessageTypeCase.NOTIFYNEWTOPICMESSAGE, new NotifyNewTopicMessageHandler(
            this,
            this.logger,
            this.repEndpoint,
            this.subEndpoint,
            this.vectorClockManager,
            this.userManager
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

    private void runSub() {
        this.subEndpoint.on(MessageTypeCase.FORWARDCHATMESSAGE, new ForwardChatMessageHandler(this.logger, this.causalDeliveryManager));
        this.subEndpoint.on(MessageTypeCase.FORWARDUSERONLINEMESSAGE, new ForwardUserOnlineMessageHandler(this.logger, this.userManager));

        while (this.running) {
            try {
                this.subEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[SUB] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private void runReactive() {
        while (this.running) {
            try {
                logger.info("[REACTIVE] Started");
                reactiveEndpoint.runServer();
            } catch (Exception e) {
                logger.error("[REACTIVE] Error while running: " + e.getMessage());
                break;
            }
        }
    }

    private void stop() {
        this.running = false;

        this.pullEndpoint.close();
        this.repEndpoint.close();
        this.subEndpoint.close();
        this.clientPubEndpoint.close();
        this.chatServerPubEndpoint.close();

        logger.info("Chat server stopped");
    }
}
