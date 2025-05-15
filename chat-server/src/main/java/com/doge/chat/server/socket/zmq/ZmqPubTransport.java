package com.doge.chat.server.socket.zmq;

import org.zeromq.ZMQ;

import com.doge.common.socket.AbstractTransport;

public class ZmqPubTransport extends AbstractTransport {
    protected final ZMQ.Socket socket;

    public ZmqPubTransport(ZMQ.Socket socket) {
        this.socket = socket;
    }

    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void send(String header, byte[] data) {
        this.socket.sendMore(header);
        this.socket.send(data);
    }

    @Override
    public byte[] receive() {
        throw new UnsupportedOperationException("[PUB] This socket does not support 'receive' operation");
    }
}
