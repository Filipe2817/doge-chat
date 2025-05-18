package com.doge.chat.server.handler;

import org.apache.commons.lang3.tuple.Pair;

import com.doge.chat.server.causal.VectorClock;
import com.doge.chat.server.user.DotSet;
import com.doge.chat.server.user.DotStore;
import com.doge.chat.server.user.OnlineUsersORSet;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
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
        ForwardUserOnlineMessage message = wrapper.getForwardUserOnlineMessage();
        String topic = message.getTopic();

        logger.debug("Started handling ForwardUserOnlineMessage for topic '" + topic + "'");

        DotStore incomingDotStore = new DotStore();
        message.getDotStoreMap().forEach((key, dotSetMessage) -> {
            DotSet dotSet = new DotSet();
            for (ForwardUserOnlineMessage.DotMessage d : dotSetMessage.getDotList()) {
                dotSet.addDot(Pair.of(d.getServerId(), d.getClock()));
            }
            incomingDotStore.put(key, dotSet);
        });

        VectorClock incomingVectorClock = new VectorClock(message.getVectorClockMap());
        OnlineUsersORSet incomingState = new OnlineUsersORSet(incomingDotStore, incomingVectorClock);

        userManager.updateDotStoreForTopic(topic, incomingState);

        logger.debug("Finished handling ForwardUserOnlineMessage for topic '" + topic + "'");
    }
}
