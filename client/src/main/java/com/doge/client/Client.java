package com.doge.client;

import java.io.IOException;
import java.util.UUID;

import com.doge.client.command.CommandManager;
import com.doge.client.command.ExitCommand;
import com.doge.client.command.HelpCommand;
import com.doge.client.command.OnlineUsersCommand;
import com.doge.client.command.SendMessageCommand;
import com.doge.client.handler.ChatMessageHandler;
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

    private PushEndpoint pushEndpoint;
    private ReqEndpoint reqEndpoint;
    private SubEndpoint subEndpoint;

    private Console console;
    private CommandManager commandManager;

    public Client(PushEndpoint pushEndpoint, ReqEndpoint reqEndpoint, SubEndpoint subEndpoint, Console console) throws IOException {
        this.id = UUID.randomUUID().toString();
        this.currentTopic = "default";
        this.running = false;

        this.pushEndpoint = pushEndpoint;
        this.reqEndpoint = reqEndpoint;
        this.subEndpoint = subEndpoint;

        this.console = console;
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

    public void run() {
        this.running = true;

        console.info("Welcome to Doge Chat!");
        console.info("Running client with id: " + id);
        
        console.info("Announcing to server...");
        this.maybeAnnounceToServer();
        
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
        
        HelpCommand helpCommand = new HelpCommand(this.commandManager);
        this.commandManager.registerCommand(helpCommand);
        
        this.commandManager.registerCommand(new ExitCommand(this, this.pushEndpoint));
        this.commandManager.registerCommand(new OnlineUsersCommand(this, this.reqEndpoint));

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

    private void maybeAnnounceToServer() {
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
                console.error("Failed to announce to server. Exiting...");
                this.stop();
                return;
            }

            console.info("Successfully announced to server. You are now online!");
        } catch (InvalidFormatException e) {
            console.error("[REQ] Error while receiving connect response: " + e.getMessage());
            return;
        }
    }

    public void stop() {
        this.running = false;

        this.pushEndpoint.close();
        this.reqEndpoint.close();
        this.subEndpoint.close();

        try {
            this.console.close();
        } catch (IOException e) {
            System.err.println("Error while closing console: " + e.getMessage());
        }

        console.info("Client stopped. Exiting...");
    }
}
