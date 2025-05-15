package com.doge.aggregation.server.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class PushEndpoint extends Endpoint<MessageWrapper> {
    public PushEndpoint(ZContext context) {
        super(new ZmqPushTransport(context.createSocket(SocketType.PUSH)), new ProtobufCodec());
    }

    public void connectSocket(String address, int port) {
        ZmqPushTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);
    }

    private ZmqPushTransport getTransportInternal() {
        return (ZmqPushTransport) super.getTransport();
    }
}
