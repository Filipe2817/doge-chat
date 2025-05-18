package com.doge.chat.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

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

    private ZContext context;
    private PullEndpoint pullEndpoint;
    private SubEndpoint subEndpoint;

    private int repPort;
    private Socket frontendSocket;
    private Socket backendSocket;
    private List<RepEndpoint> workerSockets;

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
        ZContext context,
        PullEndpoint pullEndpoint,
        SubEndpoint subEndpoint,
        int repPort,
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

        this.context = context;
        this.pullEndpoint = pullEndpoint;
        this.subEndpoint = subEndpoint;

        this.repPort = repPort;
        this.workerSockets = Collections.synchronizedList(new ArrayList<>());

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

        Thread pullThread = new Thread(this::runPull, "Pull-Thread");
        Thread subThread = new Thread(this::runSub, "Sub-Thread");
        Thread reactiveThread = new Thread(this::runReactive, "Reactive-Thread");
        Thread proxyThread = new Thread(this::runRepProxy, "Rep-Proxy-Thread");

        int workerCount = Runtime.getRuntime().availableProcessors();
        List<Thread> workerThreads = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            Thread wt = new Thread(this::runRepWorker, "Rep-Worker-" + i);
            workerThreads.add(wt);
        }

        pullThread.start();
        subThread.start();
        reactiveThread.start();

        proxyThread.start();
        workerThreads.forEach(Thread::start);
        logger.info("[REP-WORKER] All internal workers initialized");

        try {
            pullThread.join();
            subThread.join();
            reactiveThread.join();
            proxyThread.join();
            for (Thread wt : workerThreads) wt.join();
        } catch (InterruptedException e) {
            pullThread.interrupt();
            subThread.interrupt();
            reactiveThread.interrupt();
            proxyThread.interrupt();
            workerThreads.forEach(Thread::interrupt);
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

    private void runRepProxy() {
        this.frontendSocket = context.createSocket(SocketType.ROUTER);
        frontendSocket.bind("tcp://" + "localhost" + ":" + this.repPort);

        this.backendSocket = context.createSocket(SocketType.DEALER);
        backendSocket.bind("inproc://rep-workers");

        ZMQ.proxy(frontendSocket, backendSocket, null);
    }

    private void runRepWorker() {
        RepEndpoint worker = new RepEndpoint(context);
        worker.inprocConnectSocket("rep-workers");
        workerSockets.add(worker);

        worker.on(MessageTypeCase.GETONLINEUSERSMESSAGE, new GetOnlineUsersMessageHandler(
            this.logger, 
            worker,
            this.userManager
        ));
        
        worker.on(MessageTypeCase.ANNOUNCEMESSAGE, new AnnounceMessageHandler(
            this.logger,
            this.chatServerPubEndpoint,
            worker,
            this.userManager
        ));

        worker.on(MessageTypeCase.GETCHATSERVERSTATEMESSAGE, new GetChatServerStateMessageHandler(
            this,
            worker,
            this.userManager
        ));

        worker.on(MessageTypeCase.NOTIFYNEWTOPICMESSAGE, new NotifyNewTopicMessageHandler(
            this,
            this.logger,
            worker,
            this.subEndpoint,
            this.vectorClockManager,
            this.userManager
        ));

        while (this.running) {
            try {
                worker.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                logger.debug("[REP-WORKER] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private void stop() {
        this.running = false;

        if (this.pullEndpoint != null) this.pullEndpoint.close();
        if (this.subEndpoint != null) this.subEndpoint.close();
        if (this.clientPubEndpoint != null) this.clientPubEndpoint.close();
        if (this.chatServerPubEndpoint != null) this.chatServerPubEndpoint.close();

        if (this.frontendSocket != null) this.frontendSocket.close();
        if (this.backendSocket != null) this.backendSocket.close();
        for (RepEndpoint endpoint : this.workerSockets) endpoint.close();

        logger.info("Chat server stopped");
    }
}
