package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class RepEndpoint extends Endpoint<MessageWrapper> {
    public RepEndpoint(ZContext context) {
        super(new ZmqRepTransport(context.createSocket(SocketType.REP)), new ProtobufCodec());
    }

    public void bindSocket(String address, int port) {
        ZmqRepTransport transport = this.getTransportInternal();
        transport.socket.bind("tcp://" + address + ":" + port);
    }

    private ZmqRepTransport getTransportInternal() {
        return (ZmqRepTransport) super.getTransport();
    }
}
