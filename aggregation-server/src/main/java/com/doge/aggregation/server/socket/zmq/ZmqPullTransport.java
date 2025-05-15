package com.doge.aggregation.server.socket.zmq;

import org.zeromq.ZMQ;

import com.doge.common.socket.AbstractTransport;

public class ZmqPullTransport extends AbstractTransport {
    protected final ZMQ.Socket socket;

    public ZmqPullTransport(ZMQ.Socket socket) {
        this.socket = socket;
    }

    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void send(String header, byte[] data) {
        throw new UnsupportedOperationException("[PULL] This socket does not support 'send' operation");
    }

    @Override
    public byte[] receive() {
        byte[] data = socket.recv(0);
        return data;
    }
}