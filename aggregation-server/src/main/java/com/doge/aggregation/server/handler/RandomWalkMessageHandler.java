package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.cyclon.CyclonManager;
import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.RandomWalkMessage;
import com.doge.common.proto.RandomWalkMessageType;
import com.doge.common.proto.PeerEntry;

import java.util.List;

public class RandomWalkMessageHandler implements MessageHandler<MessageWrapper> {
    private final CyclonManager cyclonManager;
    private final Logger logger;

    public RandomWalkMessageHandler(CyclonManager cyclonManager, Logger logger) {
        this.cyclonManager = cyclonManager;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        if (!wrapper.hasRandomWalkMessage()) {
            logger.error("Missing random walk message in wrapper");
            return;
        }

        RandomWalkMessage shuffleMessage = wrapper.getRandomWalkMessage();
        List<PeerEntry> peerEntries = shuffleMessage.getEntriesList();
        Integer senderId = shuffleMessage.getSenderId();
        RandomWalkMessageType type = shuffleMessage.getType();
        Integer ttl = shuffleMessage.getTtl();

        logger.info("Received shuffle message from '" + senderId + "' with type " + type.name() + "");

        if (type == RandomWalkMessageType.RANDOM_WALK_REQUEST) {
            cyclonManager.handleRandomWalkRequest(senderId, ttl);
        } else if (type == RandomWalkMessageType.RANDOM_WALK_INTRO) {
            cyclonManager.handleRandomWalkIntro(senderId, ttl);
        } else if (type == RandomWalkMessageType.RANDOM_WALK_RESPONSE) {
            cyclonManager.handleRandomWalkResponse(senderId, peerEntries);
        } else {
            logger.error("Unknown shuffle message type: " + type);
            return;
        }
    }
}