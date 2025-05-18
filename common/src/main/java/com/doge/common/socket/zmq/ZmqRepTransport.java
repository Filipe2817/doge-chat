package com.doge.common.socket.zmq;

import org.zeromq.ZMQ;

import com.doge.common.socket.AbstractTransport;

public class ZmqRepTransport extends AbstractTransport {
    protected final ZMQ.Socket socket;

    public ZmqRepTransport(ZMQ.Socket socket) {
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
        byte[] data = this.socket.recv();
        return data;
    }
}
