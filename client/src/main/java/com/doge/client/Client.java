package com.doge.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZMonitor;

import com.doge.client.command.CommandManager;
import com.doge.client.command.ExitCommand;
import com.doge.client.command.HelpCommand;
import com.doge.client.command.LogsCommand;
import com.doge.client.command.OnlineUsersCommand;
import com.doge.client.command.SendMessageCommand;
import com.doge.client.command.TopicCommand;
import com.doge.client.exception.KeyNotFoundException;
import com.doge.client.handler.ChatMessageHandler;
import com.doge.client.socket.reactive.ReactiveGrpcClient;
import com.doge.client.socket.tcp.DhtClient;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.AggregationResultMessage;
import com.doge.common.proto.AnnounceMessage;
import com.doge.common.proto.AnnounceResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.proto.AggregationStartMessage;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.doge.common.socket.zmq.PushEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;
import com.doge.common.socket.zmq.SubEndpoint;

public class Client {
    private volatile boolean running;
    private final String id;

    private String currentTopic;
    private int currentChatServer;
    private List<Integer> otherChatServers;
    private static final Random RANDOM = new Random();

    private InetSocketAddress dhtNode;

    private int aggregationServerId;
    private int aggregationCount;

    private ZContext context;
    private PushEndpoint pushEndpoint;

    private ReqEndpoint chatServerReqEndpoint;
    private ZMonitor chatMonitor;

    private ReqEndpoint aggregationServerReqEndpoint;
    private SubEndpoint subEndpoint;
    private ReactiveGrpcClient reactiveClient;
    private DhtClient dhtClient;

    private Console console;
    private CommandManager commandManager;

    public Client(
        String name, 
        String topic,
        InetSocketAddress dhtNode,
        int aggregationServerId,
        ZContext context
    ) throws IOException {
        this.running = false;
        this.id = name;

        this.currentTopic = topic;
        this.currentChatServer = -1;
        this.otherChatServers = new ArrayList<>();

        this.dhtNode = dhtNode;

        this.aggregationServerId = aggregationServerId;
        this.aggregationCount = 0;

        this.context = context;

        this.console = new Console();
        console.alterSystemPrint();
        this.commandManager = new CommandManager();
    }

    public String getId() {
        return id;
    }

    public String getCurrentTopic() {
        return currentTopic;
    }

    public void setCurrentTopic(String currentTopic) {
        this.currentTopic = currentTopic;
    }

    public void setCurrentChatServer(int currentChatServer) {
        this.currentChatServer = currentChatServer;
    }

    public int updateChatServers(List<Integer> chatServers) {
        int randomIndex = RANDOM.nextInt(chatServers.size());
        int chosen = chatServers.get(randomIndex);

        this.otherChatServers.clear();
        for (Integer chatServer : chatServers) {
            if (!chatServer.equals(chosen)) {
                this.otherChatServers.add(chatServer);
            }
        }

        return chosen;
    }

    public int getCurrentChatServer() {
        return currentChatServer;
    }

    public int getRandomChatServerWithDelete() {
        if (this.otherChatServers.isEmpty()) {
            return -1;
        }

        int randomIndex = RANDOM.nextInt(this.otherChatServers.size());
        int chosen = this.otherChatServers.get(randomIndex);
        this.otherChatServers.remove(randomIndex);

        return chosen;
    }

    public int getAggregationCount() {
        return aggregationCount;
    }

    public void incrementAggregationCount() {
        this.aggregationCount++;
    }

    public void run() throws IOException {
        this.running = true;

        console.info("Welcome to Doge Chat!");
        console.info("Running client with id: " + id);

        this.dhtClient = new DhtClient(dhtNode.getHostString(), dhtNode.getPort());
        console.debug("[DHT] Connected to " + dhtNode);

        this.aggregationServerReqEndpoint = new ReqEndpoint(this.context);
        this.aggregationServerReqEndpoint.connectSocket("localhost", aggregationServerId);
        console.debug("[REQ | Aggregation server] Connected to " + aggregationServerId);

        console.info("Searching for chat servers serving topic '" + this.currentTopic + "' in DHT...");
        int chosenChatServer = this.searchByTopic();

        if (chosenChatServer == -1) {
            console.info("Starting aggregation...");

            chosenChatServer = this.startAggregation();
            if (chosenChatServer == -1) {
                console.error("Failed to aggregate");
                this.stop();
                return;
            }
        }

        this.currentChatServer = chosenChatServer;
        
        console.info("Setting up connections to chosen chat server...");
        this.setupConnectionsToChatServer(); 
        
        Thread cliThread = new Thread(this::runCli, "Cli-Thread");
        Thread subThread = new Thread(this::runSub, "Subscriber-Thread");

        try {
            cliThread.start(); 
            subThread.start();

            cliThread.join(); 
            subThread.join();
        } catch (InterruptedException e) {
            cliThread.interrupt();
            subThread.interrupt();
            this.stop();
        } 
    } 

    private void runCli() {
        this.commandManager.registerCommand(new SendMessageCommand(this, this.pushEndpoint));
        this.commandManager.registerCommand(new ExitCommand(this, this.pushEndpoint));
        this.commandManager.registerCommand(new OnlineUsersCommand(this, this.chatServerReqEndpoint));
        this.commandManager.registerCommand(new LogsCommand(this, this.reactiveClient));
        this.commandManager.registerCommand(new TopicCommand(
            this, 
            this.pushEndpoint,
            this.aggregationServerReqEndpoint,
            this.dhtClient
        ));
        
        HelpCommand helpCommand = new HelpCommand(this.commandManager);
        this.commandManager.registerCommand(helpCommand);

        // Display help on startup
        helpCommand.execute(this.console, null);

        while (this.running) {
            try {
                String line = this.console.readLine();
                if (line == null || line.isEmpty()) {
                    continue;
                }

                this.commandManager.handleCommand(this.console, line);
            } catch (Exception e) {
                console.debug("Error while reading line or handling command: " + e.getMessage());
                this.stop();
            }
        }
    }

    private void runSub() {
        this.subEndpoint.on(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(this, this.console));
        
        while (this.running) {
            try {
                this.subEndpoint.receiveOnce();
            } catch (HandlerNotFoundException | InvalidFormatException e) {
                console.debug("[SUB] Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }
    
    private int searchByTopic() {
        try {
            List<Integer> chatServers = this.dhtClient.search(this.currentTopic);
            if (chatServers.isEmpty()) {
                console.error("No chat servers available for topic '" + this.currentTopic);
                return -1;
            }

            console.warn("Found C chat servers for topic '" + this.currentTopic + "': " + chatServers);
            int chosenChatServer = this.updateChatServers(chatServers);

            console.info("Chose chat server with id '" + chosenChatServer + "' for topic '" + this.currentTopic + "'");
            return chosenChatServer;
        } catch (KeyNotFoundException e) {
            console.error("No chat server found for topic '" + this.currentTopic + "'");
            return -1;
        } catch (Exception e) {
            console.error("[DHT] Error while searching by topic: " + e.getMessage());
            return -1;
        }
    }

    private int startAggregation() {
        String dotClientId = this.id + "-" + this.aggregationCount;
        this.incrementAggregationCount();

        AggregationStartMessage startAggregationMessage = AggregationStartMessage.newBuilder()
                .setDotClientId(dotClientId)
                .setTopic(this.currentTopic)
                .build();

        MessageWrapper messageWrapper = MessageWrapper.newBuilder()
                .setAggregationStartMessage(startAggregationMessage)
                .build();

        this.aggregationServerReqEndpoint.send(messageWrapper);
        console.debug("[REQ | Aggregation server] Sent aggregation start message");

        try {
            MessageWrapper response = this.aggregationServerReqEndpoint.receiveOnceWithoutHandle();
            AggregationResultMessage result = response.getAggregationResultMessage();
            List<Integer> chatServers = result.getServerIdsList();
            console.warn("Got response from aggregation server. Found C chat servers: " + chatServers);

            int chosenChatServer = this.updateChatServers(chatServers);

            console.info("Chose chat server with id '" + chosenChatServer + "' for topic '" + this.currentTopic + "'");
            return chosenChatServer;
        } catch (Exception e) {
            console.error("[REQ | Aggregation server] Error while receiving aggregation result: " + e.getMessage());
            return -1;
        }
    }

    public void setupConnectionsToChatServer() {
        int pullPort = this.currentChatServer;
        this.pushEndpoint = new PushEndpoint(this.context);
        this.setupPushEndpoint(pullPort);
        
        int repPort = this.currentChatServer + 1;
        this.chatServerReqEndpoint = new ReqEndpoint(this.context);
        this.setupChatServerReqEndpoint(repPort, true);

        int pubPort = this.currentChatServer + 2;
        this.subEndpoint = new SubEndpoint(this.context);
        this.setupSubEndpoint(pubPort);

        int reactivePort = this.currentChatServer + 4;
        this.reactiveClient = new ReactiveGrpcClient(console);
        this.setupReactiveClient(reactivePort);

        console.info("Announcing to chat server...");
        int status = this.maybeAnnounceToServer();
        if (status == -1) {
            this.stop();
            return;
        }
    }

    public void resetConnectionsToChatServer() {
        int pullPort = this.currentChatServer;
        this.pushEndpoint.disconnectSocket();
        this.setupPushEndpoint(pullPort);

        int repPort = this.currentChatServer + 1;
        this.chatServerReqEndpoint.disconnectSocket();
        this.setupChatServerReqEndpoint(repPort, false);

        int pubPort = this.currentChatServer + 2;
        this.subEndpoint.disconnectSocket();
        this.setupSubEndpoint(pubPort);

        int reactivePort = this.currentChatServer + 4;
        this.reactiveClient.close();
        this.setupReactiveClient(reactivePort);

        console.info("Announcing to chat server...");
        int status = this.maybeAnnounceToServer();
        if (status == -1) {
            this.stop();
            return;
        }
    }

    private void setupPushEndpoint(int port) {
        this.pushEndpoint.setLinger(2000);
        this.pushEndpoint.connectSocket("localhost", port);
        console.debug("[PUSH] Connected on port " + port);
    }

    private void setupChatServerReqEndpoint(int port, boolean enableMonitor) {
        this.chatServerReqEndpoint.setHeartbeatInterval(1000);
        this.chatServerReqEndpoint.setHeartbeatTimeout(3000);
        this.chatServerReqEndpoint.setHeartbeatTTL(3000);
        this.chatServerReqEndpoint.connectSocket("localhost", port);
        
        if (enableMonitor) {
            this.chatMonitor = new ZMonitor(this.context, this.chatServerReqEndpoint.getSocket());
            chatMonitor.add(ZMonitor.Event.DISCONNECTED);
            chatMonitor.add(ZMonitor.Event.CONNECTED);
            chatMonitor.start();

            new Thread(() -> {
                while (this.running) {
                    ZMonitor.ZEvent event = chatMonitor.nextEvent(1500);
                    if (event != null && event.type == ZMonitor.Event.DISCONNECTED) {
                        console.error("[REQ | Chat server] Chat server died. Connecting to other chat server...");

                        int newChatServer = this.getRandomChatServerWithDelete();
                        if (newChatServer == -1) {
                            console.error("No other chat servers available. Stopping client...");
                            this.stop();
                            return;
                        }

                        console.info("Connecting to new chat server with id " + newChatServer);
                        this.currentChatServer = newChatServer;
                        this.resetConnectionsToChatServer();
                    }
                }
            }, "ChatServer-Monitor-Thread").start();
        }

        console.debug("[REQ | Chat server] Connected on port (also monitoring chat server) " + port);
    }

    private void setupSubEndpoint(int port) {
        this.subEndpoint.connectSocket("localhost", port);
        console.debug("[SUB] Connected on port " + port);

        this.subEndpoint.subscribe(this.currentTopic);
        console.info("You are now subscribed to topic '" + this.currentTopic + "'");
    }

    private void setupReactiveClient(int port) {
        this.reactiveClient.setup("localhost", port);
        console.debug("[REACTIVE] Connected on port " + port);
    }

    private int maybeAnnounceToServer() {
        AnnounceMessage announceMessage = AnnounceMessage.newBuilder()
                .setClientId(this.id)
                .setTopic(this.currentTopic)
                .build();

        MessageWrapper messageWrapper = MessageWrapper.newBuilder()
                .setAnnounceMessage(announceMessage)
                .build();

        this.chatServerReqEndpoint.send(messageWrapper);

        try {
            MessageWrapper response = this.chatServerReqEndpoint.receiveOnceWithoutHandle();
            AnnounceResponseMessage announceResponseMessage = response.getAnnounceResponseMessage();

            if (announceResponseMessage.getStatus() != AnnounceResponseMessage.Status.SUCCESS) {
                console.error("Failed to announce to server.");
                return -1;
            }

            console.info("Successfully announced to server. You are now online!");
            return 0;
        } catch (Exception e) {
            console.error("[REQ] Error while receiving connect response: " + e.getMessage());
            return -1;
        }
    }

    public void stop() {
        this.running = false;
        
        if (this.dhtClient != null) this.dhtClient.close();
        if (this.pushEndpoint != null) this.pushEndpoint.close();
        if (this.chatServerReqEndpoint != null) this.chatServerReqEndpoint.close();
        if (this.subEndpoint != null) this.subEndpoint.close();
        if (this.reactiveClient != null) this.reactiveClient.close();
        if (this.aggregationServerReqEndpoint != null) this.aggregationServerReqEndpoint.close();

        try {
            this.console.close();
        } catch (IOException e) {
            System.err.println("Error while closing console: " + e.getMessage());
        }

        console.info("Client stopped. Exiting...");
        System.exit(0);
    }
}
