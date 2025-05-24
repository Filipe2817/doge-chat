package com.doge.chat.server.handler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.tuple.Pair;

import com.doge.chat.server.causal.VectorClock;
import com.doge.chat.server.user.DotSet;
import com.doge.chat.server.user.DotStore;
import com.doge.chat.server.user.OnlineUsersORSet;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
import com.doge.common.proto.AnnounceMessage;
import com.doge.common.proto.AnnounceResponseMessage;
import com.doge.common.proto.ForwardUserOnlineMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;
import com.doge.common.socket.zmq.RepEndpoint;

public class AnnounceMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;

    private BlockingQueue<Pair<String, MessageWrapper>> chatServerPubQueue;
    private final RepEndpoint repEndpoint;

    private final UserManager userManager;

    public AnnounceMessageHandler(
        Logger logger,
        BlockingQueue<Pair<String, MessageWrapper>> chatServerPubQueue,
        RepEndpoint repEndpoint,
        UserManager userManager
    ) {
        this.logger = logger;

        this.chatServerPubQueue = chatServerPubQueue;
        this.repEndpoint = repEndpoint;

        this.userManager = userManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        AnnounceMessage announceMessage = wrapper.getAnnounceMessage();
        String topic = announceMessage.getTopic();
        String clientId = announceMessage.getClientId();

        logger.info("Received announce message from '" + clientId + "' on topic '" + topic + "'");
        
        this.userManager.addUserToTopic(topic, clientId);
        logger.info("Client '" + clientId + "' is now online on topic '" + topic + "'");

        DotStore dotStore = this.userManager.getDotStoreForTopic(topic);
        Set<String> onlineUsers = this.userManager.getOnlineUsersForTopic(topic);

        MessageWrapper forward = createForwardUserOnlineMessage(topic, clientId, dotStore, onlineUsers);
        try {
            this.chatServerPubQueue.put(Pair.of(topic, forward));
        } catch (InterruptedException ignored) {}

        logger.info("Forwarded announce message to topic '" + topic + "'");

        MessageWrapper response = createAnnounceResponseMessage();
        this.repEndpoint.send(response);
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
                    .setStatus(ForwardUserOnlineMessage.Status.ONLINE)
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

    private MessageWrapper createAnnounceResponseMessage() {
        AnnounceResponseMessage announceResponseMessage = AnnounceResponseMessage.newBuilder()
                .setStatus(AnnounceResponseMessage.Status.SUCCESS)
                .build();

        return MessageWrapper.newBuilder()
                .setAnnounceResponseMessage(announceResponseMessage)
                .build();
    }
}
