package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.chat.server.user.UserManager;
import com.doge.common.proto.AnnounceMessage;
import com.doge.common.proto.AnnounceResponseMessage;
import com.doge.common.proto.ForwardUserOnlineMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class AnnounceMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    private final PubEndpoint chatServerPubEndpoint;
    private final RepEndpoint repEndpoint;
    private final UserManager userManager;

    public AnnounceMessageHandler(
        Logger logger, 
        PubEndpoint chatServerPubEndpoint,
        RepEndpoint repEndpoint, 
        UserManager userManager
    ) {
        this.logger = logger;

        this.chatServerPubEndpoint = chatServerPubEndpoint;
        this.repEndpoint = repEndpoint;
        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        AnnounceMessage announceMessage = wrapper.getAnnounceMessage();
        String topic = announceMessage.getTopic();
        String clientId = announceMessage.getClientId();

        logger.info("Received announce message from '" + clientId + "' on topic '" + topic + "'");

        MessageWrapper forward = createForwardUserOnlineMessage(topic, clientId);
        this.chatServerPubEndpoint.send(topic, forward);
        logger.info("Forwarded announce message to topic '" + topic + "'");

        this.userManager.addUserToTopic(topic, clientId);
        logger.info("Client '" + clientId + "' is now online on topic '" + topic + "'");

        MessageWrapper response = createAnnounceResponseMessage();
        this.repEndpoint.send(response); 
    }

    private MessageWrapper createForwardUserOnlineMessage(String topic, String clientId) {
        ForwardUserOnlineMessage forwardUserOnlineMessage = ForwardUserOnlineMessage.newBuilder()
                .setTopic(topic)
                .setClientId(clientId)
                .setStatus(ForwardUserOnlineMessage.Status.ONLINE)
                .build();

        return MessageWrapper.newBuilder()
                .setForwardUserOnlineMessage(forwardUserOnlineMessage)
                .build();
    }

    private MessageWrapper createAnnounceResponseMessage() {
        AnnounceResponseMessage announceResponseMessage = AnnounceResponseMessage.newBuilder()
                .setStatus(AnnounceResponseMessage.Status.SUCCESS)
                .build();

        return MessageWrapper.newBuilder()
                .setAnnounceResponseMessage(announceResponseMessage)
                .build();
    }
}
