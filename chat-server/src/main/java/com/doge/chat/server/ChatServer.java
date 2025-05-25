package com.doge.chat.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang3.tuple.Pair;
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
import com.doge.common.proto.MessageWrapper;
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
    private ReactiveGrpcEndpoint reactiveEndpoint;

    private PubEndpoint chatServerPubEndpoint;

    // Pair of topic and message
    private BlockingQueue<Pair<String, MessageWrapper>> chatServerPubQueue;

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
        ReactiveGrpcEndpoint reactiveEndpoint,
        PubEndpoint chatServerPubEndpoint,
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
        this.reactiveEndpoint = reactiveEndpoint;

        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.chatServerPubQueue = new LinkedBlockingQueue<>();

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

        List<Thread> threads = new ArrayList<>();

        Thread pullThread = Thread.ofVirtual()
            .name("Pull-Thread")
            .start(this::runPull);

        threads.add(pullThread);

        Thread subThread = Thread.ofVirtual()
            .name("Sub-Thread")
            .start(this::runSub);

        threads.add(subThread);

        Thread reactiveThread = Thread.ofVirtual()
            .name("Reactive-Thread")
            .start(this::runReactive);

        threads.add(reactiveThread);

        Thread chatServerPubThread = Thread.ofVirtual()
            .name("Chat-Server-Pub-Thread")
            .start(this::runChatServerPub);

        threads.add(chatServerPubThread);

        Thread proxyThread = Thread.ofVirtual()
            .name("Rep-Proxy-Thread")
            .start(this::runRepProxy);

        threads.add(proxyThread);
        
        ThreadFactory workerFactory = Thread.ofVirtual()
            .name("Rep-Worker-Factory", 0)
            .factory();

        int workerCount = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < workerCount; i++) {
            Thread worker = workerFactory.newThread(this::runRepWorker);
            worker.start();
            threads.add(worker);
        }

        logger.info("[REP-WORKER] All internal workers initialized (proxied via port " + this.repPort + ")");

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
        this.pullEndpoint.on(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(
            this,
            this.logger,
            this.clientPubEndpoint,
            this.chatServerPubQueue,
            this.vectorClockManager,
            this.logManager
        ));

        this.pullEndpoint.on(MessageTypeCase.EXITMESSAGE, new ExitMessageHandler(
            this.logger,
            this.chatServerPubQueue,
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
        this.subEndpoint.on(MessageTypeCase.FORWARDCHATMESSAGE, new ForwardChatMessageHandler(
            this.logger, 
            this.causalDeliveryManager
        ));

        this.subEndpoint.on(MessageTypeCase.FORWARDUSERONLINEMESSAGE, new ForwardUserOnlineMessageHandler(
            this.logger, 
            this.userManager
        ));

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

    private void runChatServerPub() {
        logger.info("[PUB | Chat server queue] Started dequeuing messages");

        while (this.running) {
            try {
                Pair<String, MessageWrapper> message = this.chatServerPubQueue.take();
                String topic = message.getLeft();
                MessageWrapper wrapper = message.getRight();

                this.chatServerPubEndpoint.send(topic, wrapper);
            } catch (InterruptedException e) {
                logger.error("[PUB | Chat server queue] Interruped while dequeuing messages: " + e.getMessage());
                break;
            } catch (Exception e) {
                logger.error("[PUB | Chat server queue] Error while dequeuing messages: " + e.getMessage());
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
            this.chatServerPubQueue,
            worker,
            this.userManager
        ));

        worker.on(MessageTypeCase.GETCHATSERVERSTATEMESSAGE, new GetChatServerStateMessageHandler(
            this,
            worker,
            this.userManager,
            this.logger
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
