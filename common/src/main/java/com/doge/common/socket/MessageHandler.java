package com.doge.common.socket;

@FunctionalInterface
public interface MessageHandler<M> {
    void handle(M message);
}
