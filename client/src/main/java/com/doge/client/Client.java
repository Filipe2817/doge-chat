package com.doge.client;

import java.io.IOException;
import java.util.UUID;

import com.doge.client.command.CommandManager;
import com.doge.client.command.ExitCommand;
import com.doge.client.command.HelpCommand;
import com.doge.client.command.SendMessageCommand;
import com.doge.client.command.TopicCommand;
import com.doge.client.handler.ChatMessageHandler;
import com.doge.client.socket.PusherSocketWrapper;
import com.doge.client.socket.SubscriberSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.google.protobuf.InvalidProtocolBufferException;

public class Client {
    private volatile boolean running;
    private final String id;
    private String currentTopic;

    private PusherSocketWrapper pusherWrapper;
    private SubscriberSocketWrapper subscriberWrapper;

    private Console console;
    private CommandManager commandManager;

    public Client(PusherSocketWrapper pusherWrapper, SubscriberSocketWrapper subscriberWrapper) throws IOException {
        this.id = UUID.randomUUID().toString();
        this.currentTopic = "default";
        this.running = false;

        this.pusherWrapper = pusherWrapper;
        this.subscriberWrapper = subscriberWrapper;
        
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

    public void run() {
        this.running = true;

        System.out.println("Welcome to the Doge Chat!");
        System.out.println("Running client with id: " + id);
        
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
        this.commandManager.registerCommand(new SendMessageCommand(this, this.pusherWrapper));
        this.commandManager.registerCommand(new TopicCommand(this, this.subscriberWrapper));
        
        HelpCommand helpCommand = new HelpCommand(this.commandManager);
        this.commandManager.registerCommand(helpCommand);
        
        this.commandManager.registerCommand(new ExitCommand(this));

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
        this.subscriberWrapper.registerHandler(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(this, this.console));

        this.subscriberWrapper.subscribe(currentTopic);
        console.info("You are now subscribed to topic: " + currentTopic);
        
        while (this.running) {
            try {
                subscriberWrapper.receiveMessage();
            } catch (HandlerNotFoundException | InvalidProtocolBufferException e) {
                console.debug("Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    public void stop() {
        this.running = false;

        this.pusherWrapper.close();
        this.subscriberWrapper.close();

        try {
            this.console.close();
        } catch (IOException e) {
            System.err.println("Error while closing console: " + e.getMessage());
        }

        console.info("Client stopped");
    }
}
