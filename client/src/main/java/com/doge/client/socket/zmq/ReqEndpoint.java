package com.doge.client.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class ReqEndpoint extends Endpoint<MessageWrapper> {
    public ReqEndpoint(ZContext context) {
        super(new ZmqReqTransport(context.createSocket(SocketType.REQ)), new ProtobufCodec());
    }

    public void connectSocket(String address, int port) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);
    }

    private ZmqReqTransport getTransportInternal() {
        return (ZmqReqTransport) super.getTransport();
    }
}
