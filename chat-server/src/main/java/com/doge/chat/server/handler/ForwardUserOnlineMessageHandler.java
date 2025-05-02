package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.user.UserManager;
import com.doge.common.proto.ForwardUserOnlineMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ForwardUserOnlineMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    private final UserManager userManager;

    public ForwardUserOnlineMessageHandler(Logger logger, UserManager userManager) {
        this.logger = logger;
        
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ForwardUserOnlineMessage forwardUserOnlineMessage = wrapper.getForwardUserOnlineMessage();
        String clientId = forwardUserOnlineMessage.getClientId();
        String topic = forwardUserOnlineMessage.getTopic();

        logger.info("Received forwarded user online message from '" + clientId + "' on topic '" + topic + "'");

        if (forwardUserOnlineMessage.getStatus() == ForwardUserOnlineMessage.Status.ONLINE) {
            userManager.addUserToTopic(topic, clientId);
            logger.info("Client '" + clientId + "' is now online on topic '" + topic + "'");
        } else {
            userManager.removeUserFromTopic(topic, clientId);
            logger.info("Client '" + clientId + "' is now offline on topic '" + topic + "'");
        }
    }
}
