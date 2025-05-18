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
    private Logger logger;

    private RepEndpoint repEndpoint;
    private UserManager userManager;

    public GetChatServerStateMessageHandler(
        ChatServer chatServer,
        Logger logger, 
        RepEndpoint repEndpoint, 
        UserManager userManager
    ) {
        this.chatServer = chatServer;
        this.logger = logger;

        this.repEndpoint = repEndpoint;
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper message) {
        int userCount = userManager.getTotalUsers();
        // FIXME: Replace this after the chat server is informed by the aggregation server
        // that it is now serving a topic | Maybe create a TopicManager?
        int topicCount = 1;

        logger.info("Chat server has a total of " + userCount + " users and is serving a total of " + topicCount + " topics");
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
