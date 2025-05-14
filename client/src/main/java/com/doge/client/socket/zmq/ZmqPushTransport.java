package com.doge.client.socket.zmq;

import org.zeromq.ZMQ;

import com.doge.common.socket.AbstractTransport;

public class ZmqPushTransport extends AbstractTransport {
    protected final ZMQ.Socket socket;

    public ZmqPushTransport(ZMQ.Socket socket) {
        this.socket = socket;
    }

    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void send(String header, byte[] data) {
        this.socket.send(data);
    }

    @Override
    public byte[] receive() {
        throw new UnsupportedOperationException("[PUSH] This socket does not support 'receive' operation");
    }
}
