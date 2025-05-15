package com.doge.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import org.zeromq.ZContext;

import com.doge.client.command.CommandManager;
import com.doge.client.command.ExitCommand;
import com.doge.client.command.HelpCommand;
import com.doge.client.command.LogsCommand;
import com.doge.client.command.OnlineUsersCommand;
import com.doge.client.command.SendMessageCommand;
import com.doge.client.exception.KeyNotFoundException;
import com.doge.client.handler.ChatMessageHandler;
import com.doge.client.socket.reactive.ReactiveGrpcClient;
import com.doge.client.socket.tcp.DhtClient;
import com.doge.client.socket.zmq.PushEndpoint;
import com.doge.client.socket.zmq.ReqEndpoint;
import com.doge.client.socket.zmq.SubEndpoint;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.AnnounceMessage;
import com.doge.common.proto.AnnounceResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class Client {
    private volatile boolean running;

    private final String id;
    private String currentTopic;
    private int currentChatServer;
    private InetSocketAddress dhtNode;

    private ZContext context;
    private PushEndpoint pushEndpoint;
    private ReqEndpoint reqEndpoint;
    private SubEndpoint subEndpoint;
    private ReactiveGrpcClient reactiveClient;
    private DhtClient dhtClient;

    private Console console;
    private CommandManager commandManager;

    public Client(
        String name, 
        String topic,
        InetSocketAddress dhtNode,
        ZContext context
    ) throws IOException {
        this.running = false;

        this.id = name;
        this.currentTopic = topic;
        this.dhtNode = dhtNode;

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

    public void run() throws IOException {
        this.running = true;

        console.info("Welcome to Doge Chat!");
        console.info("Running client with id: " + id);

        this.dhtClient = new DhtClient(dhtNode.getHostString(), dhtNode.getPort());
        console.debug("[DHT] Connected to " + dhtNode);
        console.info("Searching for chat servers serving topic '" + this.currentTopic + "' in DHT...");
        int chosenChatServer = this.searchByTopic();
        if (chosenChatServer == -1) {
            // TODO: Start aggregation at this point
            this.stop();
            return;
        }
        this.currentChatServer = chosenChatServer;
        
        console.info("Setting up connections to chosen chat server...");
        this.setupConnectionsToChatServer();

        console.info("Announcing to chat server...");
        int status = this.maybeAnnounceToServer();
        if (status == -1) {
            this.stop();
            return;
        }
        
        Thread cliThread = new Thread(() -> this.runCli(), "Cli-Thread");
        Thread subscriberThread = new Thread(() -> this.runSubscriber(), "Subscriber-Thread");

        try {
            cliThread.start(); 
            subscriberThread.start();

            cliThread.join(); 
            subscriberThread.join();
        } catch (InterruptedException e) {
            cliThread.interrupt();
            subscriberThread.interrupt();
            this.stop();
        } 
    } 

    private void runCli() {
        this.commandManager.registerCommand(new SendMessageCommand(this, this.pushEndpoint));
        this.commandManager.registerCommand(new ExitCommand(this, this.pushEndpoint));
        this.commandManager.registerCommand(new OnlineUsersCommand(this, this.reqEndpoint));
        this.commandManager.registerCommand(new LogsCommand(this, this.reactiveClient));
        
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

    private void runSubscriber() {
        this.subEndpoint.on(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(this, this.console));

        this.subEndpoint.subscribe(this.currentTopic);
        console.info("You are now subscribed to topic " + "'" + this.currentTopic + "'");
        
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

            // Randomly select a chat server from the list
            Random random = new Random();
            int randomIndex = random.nextInt(chatServers.size());
            int chosenChatServer = chatServers.get(randomIndex);

            console.info("Found chat server with id '" + chosenChatServer + "' for topic '" + this.currentTopic + "'");
            return chosenChatServer;
        } catch (KeyNotFoundException e) {
            console.error("No chat server found for topic '" + this.currentTopic + "'. Starting aggregation...");
            return -1;
        } catch (Exception e) {
            console.error("Error while searching by topic: " + e.getMessage());
            return -1;
        }
    }

    private void setupConnectionsToChatServer() {
        int pullPort = this.currentChatServer;
        this.pushEndpoint = new PushEndpoint(this.context);
        pushEndpoint.connectSocket("localhost", pullPort);
        console.debug("[PULL] Connected on port " + pullPort);
        
        int repPort = this.currentChatServer + 1;
        this.reqEndpoint = new ReqEndpoint(this.context);
        reqEndpoint.connectSocket("localhost", repPort);
        console.debug("[REQ] Connected on port " + repPort);

        int pubPort = this.currentChatServer + 2;
        this.subEndpoint = new SubEndpoint(this.context);
        subEndpoint.connectSocket("localhost", pubPort);
        console.debug("[SUB] Connected on port " + pubPort);

        int reactivePort = this.currentChatServer + 4;
        this.reactiveClient = new ReactiveGrpcClient(console);
        reactiveClient.setup("localhost", reactivePort);
        console.debug("[REACTIVE] Connected on port " + reactivePort);
    }

    private int maybeAnnounceToServer() {
        AnnounceMessage announceMessage = AnnounceMessage.newBuilder()
                .setClientId(this.id)
                .setTopic(this.currentTopic)
                .build();

        MessageWrapper messageWrapper = MessageWrapper.newBuilder()
                .setAnnounceMessage(announceMessage)
                .build();

        this.reqEndpoint.send(messageWrapper);

        try {
            MessageWrapper response = this.reqEndpoint.receiveOnceWithoutHandle();
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

        if (this.pushEndpoint != null) this.pushEndpoint.close();
        if (this.reqEndpoint != null) this.reqEndpoint.close();
        if (this.subEndpoint != null) this.subEndpoint.close();

        try {
            this.console.close();
        } catch (IOException e) {
            System.err.println("Error while closing console: " + e.getMessage());
        }

        console.info("Client stopped. Exiting...");
    }
}
