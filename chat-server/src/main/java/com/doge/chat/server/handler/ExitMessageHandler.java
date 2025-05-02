package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.common.proto.ExitMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ExitMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    public ExitMessageHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper message) {
        ExitMessage exitMessage = message.getExitMessage();
        String clientId = exitMessage.getClientId();

        logger.info("Received exit message from client '" + clientId + "'");

        // TODO: Remove client from the users state
    }
}
