package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class PubEndpoint extends Endpoint<MessageWrapper> {
    public PubEndpoint(ZContext context) {
        super(new ZmqPubTransport(context.createSocket(SocketType.PUB)), new ProtobufCodec());
    }

    public void bindSocket(String address, int port) {
        ZmqPubTransport transport = this.getTransportInternal();
        transport.socket.bind("tcp://" + address + ":" + port);
    }

    private ZmqPubTransport getTransportInternal() {
        return (ZmqPubTransport) super.getTransport();
    }
}
