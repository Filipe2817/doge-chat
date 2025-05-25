package com.doge.chat.server.handler;

import com.doge.chat.server.ChatServer;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
import com.doge.common.proto.ChatServerStateMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.RepEndpoint;

public class GetChatServerStateMessageHandler implements MessageHandler<MessageWrapper> {
    private ChatServer chatServer;

    private RepEndpoint repEndpoint;
    private UserManager userManager;
    
    private Logger logger;

    public GetChatServerStateMessageHandler(
        ChatServer chatServer,
        RepEndpoint repEndpoint, 
        UserManager userManager,
        Logger logger
    ) {
        this.chatServer = chatServer;

        this.repEndpoint = repEndpoint;
        this.userManager = userManager;

        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper message) {
        int userCount = userManager.getTotalUsers();
        int topicCount = chatServer.getTopicsBeingServed();

        logger.info("Received request for chat server state: " + userCount + " users, " + topicCount + " topics");

        MessageWrapper responseMessage = createChatServerStateMessage(userCount, topicCount);
        repEndpoint.send(responseMessage);
    }

    private MessageWrapper createChatServerStateMessage(int userCount, int topicCount) {
        ChatServerStateMessage message = ChatServerStateMessage.newBuilder()
                .setId(chatServer.getId())
                .setUserCount(userCount)
                .setTopicCount(topicCount)
                .build();

        return MessageWrapper.newBuilder()
                .setChatServerStateMessage(message)
                .build();
    }
}
