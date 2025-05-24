package com.doge.client.command;

import java.util.List;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.GetOnlineUsersMessage;
import com.doge.common.proto.GetOnlineUsersResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.zmq.ReqEndpoint;

public class OnlineUsersCommand extends AbstractCommand {
    private final Client client;

    private final ReqEndpoint reqEndpoint;

    public OnlineUsersCommand(Client client, ReqEndpoint reqEndpoint) {
        super("/online");
        this.client = client;
        
        this.reqEndpoint = reqEndpoint;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length > 0) {
            sendUsage(console);
            return;
        }

        console.info("Fetching online users...");

        String topic = client.getCurrentTopic();
        String clientId = client.getId();

        MessageWrapper wrapper = this.createGetOnlineUsersMessage(topic, clientId);
        this.reqEndpoint.send(wrapper);
        
        try {
            MessageWrapper responseWrapper = this.reqEndpoint.receiveOnceWithoutHandle();
            GetOnlineUsersResponseMessage response = responseWrapper.getGetOnlineUsersResponseMessage();

            this.printOnlineUsers(console, response.getOnlineUsersList(), topic);
        } catch (InvalidFormatException e) {
            console.error("Error while fetching online users: " + e.getMessage());
        }
    }

    private void printOnlineUsers(Console console, List<String> onlineUsers, String topic) {
        if (onlineUsers.isEmpty()) {
            console.info("No users are online for topic " + "'" + topic + "'");
        } else {
            console.info("Online users for topic " + "'" + topic + "':");
            for (String user : onlineUsers) {
                if (user.equals(client.getId())) {
                    console.info("- " + user + " (you)");
                } else {
                    console.info("- " + user);
                }
            }
        }
    }

    private MessageWrapper createGetOnlineUsersMessage(String topic, String clientId) {
        GetOnlineUsersMessage getOnlineUsers = GetOnlineUsersMessage.newBuilder()
                .setClientId(clientId)
                .setTopic(topic)
                .build();

        return MessageWrapper.newBuilder()
                .setGetOnlineUsersMessage(getOnlineUsers)
                .build();
    }
}
