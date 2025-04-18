package com.doge.common.exception;

import com.doge.common.proto.MessageWrapper.MessageTypeCase;

public class HandlerNotFoundException extends Exception {
    public HandlerNotFoundException(MessageTypeCase messageType) {
        super("Handler not found for message type " + messageType);
    }
}
