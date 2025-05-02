package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.chat.server.user.UserManager;
import com.doge.common.proto.ExitMessage;
import com.doge.common.proto.ForwardUserOnlineMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ExitMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    private final PubEndpoint chatServerPubEndpoint;
    private final UserManager userManager;

    public ExitMessageHandler(Logger logger, PubEndpoint chatServerPubEndpoint, UserManager userManager) {
        this.logger = logger;

        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper message) {
        ExitMessage exitMessage = message.getExitMessage();
        String clientId = exitMessage.getClientId();
        String topic = exitMessage.getTopic();

        logger.info("Received exit message from client '" + clientId + "' on topic '" + topic + "'");

        MessageWrapper forward = createForwardUserOnlineMessage(topic, clientId);
        this.chatServerPubEndpoint.send(topic, forward);
        logger.info("Forwarded exit message to topic '" + topic + "'");

        this.userManager.removeUserFromTopic(topic, clientId);
        logger.info("Client '" + clientId + "' is now offline on topic '" + topic + "'");
    }

    private MessageWrapper createForwardUserOnlineMessage(String topic, String clientId) {
        ForwardUserOnlineMessage forwardUserOnlineMessage = ForwardUserOnlineMessage.newBuilder()
                .setTopic(topic)
                .setClientId(clientId)
                .setStatus(ForwardUserOnlineMessage.Status.OFFLINE)
                .build();

        return MessageWrapper.newBuilder()
                .setForwardUserOnlineMessage(forwardUserOnlineMessage)
                .build();
    }
}
