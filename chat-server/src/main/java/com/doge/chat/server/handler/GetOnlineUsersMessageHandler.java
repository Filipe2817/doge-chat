package com.doge.chat.server.handler;

import java.util.List;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.common.proto.GetOnlineUsersMessage;
import com.doge.common.proto.GetOnlineUsersResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class GetOnlineUsersMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;
    
    private final RepEndpoint repEndpoint;

    public GetOnlineUsersMessageHandler(Logger logger, RepEndpoint repEndpoint) {
        this.logger = logger;
        
        this.repEndpoint = repEndpoint;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        GetOnlineUsersMessage getOnlineUsersMessage = wrapper.getGetOnlineUsersMessage();
        String topic = getOnlineUsersMessage.getTopic();
        String clientId = getOnlineUsersMessage.getClientId();

        logger.info("Received request for online users from '" + clientId + "' on topic '" + topic + "'");

        // FIXME: Send response back to the client with the list of really online users
        List<String> onlineUsers = List.of("Rui", "Pedro");
        MessageWrapper response = createGetOnlineUsersResponseMessage(onlineUsers);
        this.repEndpoint.send(response);
    }

    private MessageWrapper createGetOnlineUsersResponseMessage(List<String> onlineUsers) {
        GetOnlineUsersResponseMessage getOnlineUsersResponseMessage = GetOnlineUsersResponseMessage.newBuilder()
                .addAllOnlineUsers(onlineUsers)
                .build();

        return MessageWrapper.newBuilder()
                .setGetOnlineUsersResponseMessage(getOnlineUsersResponseMessage)
                .build();
    }
}
