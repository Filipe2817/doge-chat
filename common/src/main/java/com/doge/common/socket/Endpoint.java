package com.doge.common.socket;

import java.util.HashMap;
import java.util.Map;

import com.doge.common.codec.MessageCodec;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.exception.InvalidFormatException;

public class Endpoint<M> implements AutoCloseable {
    private final AbstractTransport transport;
    private final MessageCodec<M> codec;
    private final Map<Object, MessageHandler<M>> handlers;

    public Endpoint(AbstractTransport transport, MessageCodec<M> codec) {
        this.transport = transport;
        this.codec = codec;
        this.handlers = new HashMap<>();
    }

    public AbstractTransport getTransport() {
        return this.transport;
    }

    public MessageCodec<M> getCodec() {
        return this.codec;
    }

    public void on(Object messageType, MessageHandler<M> handler) {
        this.handlers.put(messageType, handler);
    }

    public void send(M message) {
        send("", message);
    }

    public void send(String header, M message) {
        byte[] encodedMessage = this.codec.encode(message);
        this.transport.send(header, encodedMessage);
    }

    public void receiveOnce() throws InvalidFormatException, HandlerNotFoundException {
        byte[] rawMessage = this.transport.receive();
        M message = this.codec.decode(rawMessage);

        Object type = this.codec.getMessageType(message);
        MessageHandler<M> handler = this.handlers.get(type);
        if (handler == null) throw new HandlerNotFoundException(type);

        handler.handle(message);
    }

    @Override
    public void close() {
        this.transport.close();
    }
}
