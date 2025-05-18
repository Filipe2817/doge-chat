package com.doge.chat.server.handler;

import java.util.ArrayList;
import java.util.List;

import com.doge.chat.server.ChatServer;
import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.proto.NotifyNewTopicAckMessage;
import com.doge.common.proto.NotifyNewTopicMessage;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.SubEndpoint;

public class NotifyNewTopicMessageHandler implements MessageHandler<MessageWrapper> {
    private ChatServer chatServer;
    private Logger logger;
    
    private RepEndpoint repEndpoint;
    private SubEndpoint subEndpoint;

    private VectorClockManager vectorClockManager;
    private UserManager userManager;

    public NotifyNewTopicMessageHandler(
        ChatServer chatServer,
        Logger logger,
        RepEndpoint repEndpoint,
        SubEndpoint subEndpoint,
        VectorClockManager vectorClockManager,
        UserManager userManager
    ) {
        this.chatServer = chatServer;
        this.logger = logger;

        this.repEndpoint = repEndpoint;
        this.subEndpoint = subEndpoint;

        this.vectorClockManager = vectorClockManager;
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        NotifyNewTopicMessage message = wrapper.getNotifyNewTopicMessage();
        List<Integer> chatServerIds = message.getOtherServerIdsList();

        for (Integer id : chatServerIds) {
            int pubPort = id + 3;
            subEndpoint.connectSocket("localhost", pubPort);
            logger.debug("[SUB] Connected to port " + pubPort);
        }

        List<Integer> fullIds = new ArrayList<>(chatServerIds);
        fullIds.add(chatServer.getId());
        
        String topic = message.getTopic();
        subEndpoint.subscribe(topic);
        chatServer.incrementTopicsBeingServed();
        logger.info("[SUB] Chat server is now part of topic " + "'" + topic + "'");

        vectorClockManager.addTopic(topic, fullIds);
        logger.debug("Vector clock manager initialized for topic " + "'" + topic + "' with identifiers " + fullIds);

        userManager.addTopic(topic, fullIds);
        logger.debug("User manager initialized for topic " + "'" + topic + "' with identifiers " + fullIds);

        MessageWrapper response = createNotifyNewTopicAckMessage(topic);
        repEndpoint.send(response);
    }

    private MessageWrapper createNotifyNewTopicAckMessage(String topic) {
        NotifyNewTopicAckMessage message = NotifyNewTopicAckMessage.newBuilder()
            .setTopic(topic)
            .build();

        return MessageWrapper.newBuilder()
            .setNotifyNewTopicAckMessage(message)
            .build();
    }
}
