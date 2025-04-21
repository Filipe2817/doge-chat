package com.doge.client.socket;

import org.zeromq.ZMQ;

import com.doge.common.socket.AbstractTransport;

public class ZmqSubTransport extends AbstractTransport {
    protected final ZMQ.Socket socket;

    public ZmqSubTransport(ZMQ.Socket socket) {
        this.socket = socket;
    }
    
    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void send(String header, byte[] data) {
        throw new UnsupportedOperationException("SUB socket does not support send operation");
    }

    @Override
    public byte[] receive() {
        this.socket.recvStr();
        byte[] data = this.socket.recv(0);
        return data;
    }
}
