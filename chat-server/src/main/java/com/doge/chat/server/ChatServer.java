package com.doge.chat.server;

import com.doge.chat.server.handler.ChatMessageHandler;
import com.doge.chat.server.handler.ForwardChatMessageHandler;
import com.doge.chat.server.socket.PublisherSocketWrapper;
import com.doge.chat.server.socket.PullerSocketWrapper;
import com.doge.chat.server.socket.SubscriberSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.google.protobuf.InvalidProtocolBufferException;

public class ChatServer {
    private volatile boolean running;
    private final String topic;

    private PullerSocketWrapper pullerWrapper;
    private SubscriberSocketWrapper subscriberWrapper;
    private PublisherSocketWrapper clientPublisherWrapper;
    private PublisherSocketWrapper chatServerPublisherWrapper;

    private final Logger logger;

    public ChatServer(
        String topic,
        PullerSocketWrapper pullerWrapper, 
        SubscriberSocketWrapper subscriberWrapper,
        PublisherSocketWrapper clientPublisherWrapper,
        PublisherSocketWrapper chatServerPublisherWrapper
    ) {
        this.topic = topic;
        this.running = false;

        this.pullerWrapper = pullerWrapper;
        this.subscriberWrapper = subscriberWrapper;
        this.clientPublisherWrapper = clientPublisherWrapper;
        this.chatServerPublisherWrapper = chatServerPublisherWrapper;

        this.logger = new Logger();
    }

    public void run() {
        this.running = true;
        logger.info("Started chat server serving topic: " + this.topic);

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
        this.pullerWrapper.registerHandler(MessageTypeCase.CHATMESSAGE, new ChatMessageHandler(this.chatServerPublisherWrapper, this.logger));

        while (this.running) {
            try {
                pullerWrapper.receiveMessage();
            } catch (HandlerNotFoundException | InvalidProtocolBufferException e) {
                logger.debug("Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void runSubscriber() {
        this.subscriberWrapper.registerHandler(MessageTypeCase.FORWARDCHATMESSAGE, new ForwardChatMessageHandler(this.clientPublisherWrapper, this.logger));

        this.subscriberWrapper.subscribe(this.topic);
        logger.info("Chat server is now subscribed to topic: " + this.topic);

        while (this.running) {
            try {
                subscriberWrapper.receiveMessage();
            } catch (HandlerNotFoundException | InvalidProtocolBufferException e) {
                logger.debug("Error while receiving message: " + e.getMessage());
                continue;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void stop() {
        this.running = false;
        
        this.pullerWrapper.close();
        this.subscriberWrapper.close();
        this.clientPublisherWrapper.close();
        this.chatServerPublisherWrapper.close();
        
        logger.info("Chat server stopped");
    }
}
