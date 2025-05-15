package com.doge.aggregation.server.socket.zmq;

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
        socket.setSendTimeOut(5000);
        boolean sent = socket.send(data, 0);
        if (!sent) {
            throw new RuntimeException("Send timed out after 5000ms");
        }
    }


    @Override
    public byte[] receive() {
        throw new UnsupportedOperationException("[PUSH] This socket does not support 'receive' operation");
    }
}
