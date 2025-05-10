package com.doge.aggregation.server.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class PullEndpoint extends Endpoint<MessageWrapper> {
    public PullEndpoint(ZContext context) {
        super(new ZmqPullTransport(context.createSocket(SocketType.PULL)), new ProtobufCodec());
    }

    public void bindSocket(String address, int port) {
        ZmqPullTransport transport = this.getTransportInternal();
        transport.socket.bind("tcp://" + address + ":" + port);
    }

    private ZmqPullTransport getTransportInternal() {
        return (ZmqPullTransport) super.getTransport();
    }
}
