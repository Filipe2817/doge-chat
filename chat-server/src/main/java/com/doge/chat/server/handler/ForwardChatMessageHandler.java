package com.doge.chat.server.handler;

import com.doge.chat.server.Logger;
import com.doge.chat.server.causal.CausalDeliveryManager;
import com.doge.chat.server.logs.LogsManager;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.ForwardChatMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.MessageHandler;

public class ForwardChatMessageHandler implements MessageHandler<MessageWrapper> {
    private final Logger logger;
    
    private final CausalDeliveryManager causalDeliveryManager;

    public ForwardChatMessageHandler(Logger logger, CausalDeliveryManager causalDeliveryManager) {
        this.logger = logger;
        
        this.causalDeliveryManager = causalDeliveryManager;
    }

    @Override
    public void handle(MessageWrapper wrapper) {
        ForwardChatMessage forwardChatMessage = wrapper.getForwardChatMessage();
        ChatMessage chatMessage = forwardChatMessage.getChatMessage();
        String topic = chatMessage.getTopic();
        String clientId = chatMessage.getClientId();
        String content = chatMessage.getContent();

        logger.info("Received forwarded message from '" + clientId + "' on topic '" + topic + "' with content: " + content);

        this.causalDeliveryManager.addAndMaybeDeliver(forwardChatMessage);
    }
}
