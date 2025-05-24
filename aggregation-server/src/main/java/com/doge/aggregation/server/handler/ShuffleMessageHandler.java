package com.doge.aggregation.server.handler;

import com.doge.aggregation.server.cyclon.CyclonManager;
import com.doge.common.Logger;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.proto.ShuffleMessage;
import com.doge.common.proto.ShuffleMessageType;
import com.doge.common.proto.PeerEntry;

import java.util.List;

public class ShuffleMessageHandler implements MessageHandler<MessageWrapper> {
    private final CyclonManager cyclonManager;
    private final Logger logger;

    public ShuffleMessageHandler(
        CyclonManager cyclonManager,
        Logger logger
    ) {
        this.cyclonManager = cyclonManager;
        this.logger = logger;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        if (!wrapper.hasShuffleMessage()) {
            logger.error("Missing shuffle message in wrapper");
            return;
        }

        ShuffleMessage shuffleMessage = wrapper.getShuffleMessage();
        List<PeerEntry> peerEntries = shuffleMessage.getEntriesList();
        Integer senderId = shuffleMessage.getSenderId();
        ShuffleMessageType type = shuffleMessage.getType();

        logger.info("Received shuffle message from '" + senderId + "' with type " + type.name() + "");

        if (type == ShuffleMessageType.SHUFFLE_REQUEST) {
            cyclonManager.handleShuffleRequest(senderId, peerEntries);
        } else if (type == ShuffleMessageType.SHUFFLE_RESPONSE) {
            cyclonManager.handleShuffleResponse(senderId, peerEntries);
        } else {
            logger.error("Unknown shuffle message type: " + type);
            return;
        }
    }
}
