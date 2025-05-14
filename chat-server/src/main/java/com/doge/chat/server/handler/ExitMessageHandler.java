package com.doge.chat.server.handler;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.doge.chat.server.Logger;
import com.doge.chat.server.causal.VectorClock;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.chat.server.user.DotSet;
import com.doge.chat.server.user.DotStore;
import com.doge.chat.server.user.OnlineUsersORSet;
import com.doge.chat.server.user.UserManager;
import com.doge.common.proto.ExitMessage;
import com.doge.common.proto.ForwardUserOnlineMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

// TODO: Same logic as AnnounceMessageHandler, but for exit messages
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

        this.userManager.removeUserFromTopic(topic, clientId);
        logger.info("Client '" + clientId + "' is now offline on topic '" + topic + "'");

        DotStore dotStore = this.userManager.getDotStoreForTopic(topic);
        Set<String> onlineUsers = this.userManager.getOnlineUsersForTopic(topic);

        MessageWrapper forward = createForwardUserOnlineMessage(topic, clientId, dotStore, onlineUsers);
        this.chatServerPubEndpoint.send(topic, forward);
        logger.info("Forwarded exit message to topic '" + topic + "'");
    }
    
    private MessageWrapper createForwardUserOnlineMessage(
        String topic,
        String clientId,
        DotStore dotStore,
        Set<String> onlineUsers
    ) {
        OnlineUsersORSet onlineUsersORSet = this.userManager.getOnlineUsersORSetForTopic(topic);
        VectorClock vectorClock = onlineUsersORSet.getVectorClock();

        ForwardUserOnlineMessage.Builder builder = ForwardUserOnlineMessage.newBuilder()
                    .setTopic(topic)
                    .setClientId(clientId)
                    .setStatus(ForwardUserOnlineMessage.Status.OFFLINE)
                    .putAllVectorClock(vectorClock.asData());

        for (Map.Entry<String, DotSet> entry : dotStore.entrySet()) {
            String userId = entry.getKey();
            DotSet dots = entry.getValue();

            ForwardUserOnlineMessage.DotSetMessage.Builder dotSetBuilder = 
                ForwardUserOnlineMessage.DotSetMessage.newBuilder();

            for (Pair<Integer, Integer> dot : dots) {
                ForwardUserOnlineMessage.DotMessage dotMessage =
                    ForwardUserOnlineMessage.DotMessage.newBuilder()
                        .setServerId(dot.getLeft())
                        .setClock(dot.getRight())
                        .build();

                dotSetBuilder.addDot(dotMessage);
            }
            
            builder.putDotStore(userId, dotSetBuilder.build());
        }


        ForwardUserOnlineMessage forward = builder.build();
        return MessageWrapper.newBuilder()
                .setForwardUserOnlineMessage(forward)
                .build();
    }
}
