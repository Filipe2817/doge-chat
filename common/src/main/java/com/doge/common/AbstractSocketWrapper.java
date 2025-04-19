package com.doge.common;

import java.util.HashMap;
import java.util.Map;

import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.proto.MessageWrapper.MessageTypeCase;
import com.google.protobuf.InvalidProtocolBufferException;

public abstract class AbstractSocketWrapper<S> {
    protected final S socket;
    private final Map<MessageTypeCase, Handler> handlers;

    public AbstractSocketWrapper(S socket) {
        this.socket = socket;
        this.handlers = new HashMap<>();
    }

    public abstract void close();

    public void registerHandler(MessageTypeCase messageType, Handler handler) {
        this.handlers.put(messageType, handler);
    }

    public void unregisterHandler(MessageTypeCase messageType) {
        this.handlers.remove(messageType);
    }

    public abstract void sendMessage(String header, MessageWrapper wrapper);
    
    public void sendMessage(MessageWrapper wrapper) {
        sendMessage("", wrapper);
    }

    public abstract void receiveMessage() throws HandlerNotFoundException, InvalidProtocolBufferException;
    
    protected void runHandler(MessageTypeCase messageType, MessageWrapper message) throws HandlerNotFoundException, InvalidProtocolBufferException {
        Handler handler = handlers.get(messageType);
        if (handler != null) {
            handler.handle(message);
        } else {
            throw new HandlerNotFoundException(messageType);
        }
    }
}
