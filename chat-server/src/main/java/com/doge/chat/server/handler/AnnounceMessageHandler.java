package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.common.proto.AnnounceMessage;
import com.doge.common.proto.AnnounceResponseMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class AnnounceMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    private final RepEndpoint repEndpoint;

    public AnnounceMessageHandler(Logger logger, RepEndpoint repEndpoint) {
        this.logger = logger;

        this.repEndpoint = repEndpoint;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        AnnounceMessage announceMessage = wrapper.getAnnounceMessage();
        String topic = announceMessage.getTopic();
        String clientId = announceMessage.getClientId();

        logger.info("Received announce message from '" + clientId + "' on topic '" + topic + "'");

        MessageWrapper response = createAnnounceResponseMessage();
        this.repEndpoint.send(response); 
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
