package com.doge.chat.server.handler;

import java.util.Set;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.chat.server.user.UserManager;
import com.doge.common.proto.GetOnlineUsersMessage;
import com.doge.common.proto.GetOnlineUsersResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class GetOnlineUsersMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;
    
    private final RepEndpoint repEndpoint;
    private final UserManager userManager;

    public GetOnlineUsersMessageHandler(Logger logger, RepEndpoint repEndpoint, UserManager userManager) {
        this.logger = logger;
        
        this.repEndpoint = repEndpoint;
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        GetOnlineUsersMessage getOnlineUsersMessage = wrapper.getGetOnlineUsersMessage();
        String topic = getOnlineUsersMessage.getTopic();
        String clientId = getOnlineUsersMessage.getClientId();

        logger.info("Received request for online users from '" + clientId + "' on topic '" + topic + "'");

        Set<String> onlineUsers = this.userManager.getOnlineUsersForTopic(topic);
        MessageWrapper response = createGetOnlineUsersResponseMessage(onlineUsers);
        this.repEndpoint.send(response);
    }

    private MessageWrapper createGetOnlineUsersResponseMessage(Set<String> onlineUsers) {
        GetOnlineUsersResponseMessage getOnlineUsersResponseMessage = GetOnlineUsersResponseMessage.newBuilder()
                .addAllOnlineUsers(onlineUsers)
                .build();

        return MessageWrapper.newBuilder()
                .setGetOnlineUsersResponseMessage(getOnlineUsersResponseMessage)
                .build();
    }
}
