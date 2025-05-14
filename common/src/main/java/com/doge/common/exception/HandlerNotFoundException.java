package com.doge.common.exception;

public class HandlerNotFoundException extends Exception {
    public HandlerNotFoundException(Object messageType) {
        super("Handler not found for message type " + messageType);
    }
}
