package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class PushEndpoint extends Endpoint<MessageWrapper> {
    private String address;
    private int port;

    public PushEndpoint(ZContext context) {
        super(new ZmqPushTransport(context.createSocket(SocketType.PUSH)), new ProtobufCodec());
    }

    public void connectSocket(String address, int port) {
        ZmqPushTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);

        this.address = address;
        this.port = port;
    }

    public void disconnectSocket() {
        ZmqPushTransport transport = this.getTransportInternal();
        transport.socket.disconnect("tcp://" + address + ":" + port);
    }

    public void setLinger(int linger) {
        ZmqPushTransport transport = this.getTransportInternal();
        transport.socket.setLinger(linger);
    }

    private ZmqPushTransport getTransportInternal() {
        return (ZmqPushTransport) super.getTransport();
    }
}
