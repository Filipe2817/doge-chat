package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class ReqEndpoint extends Endpoint<MessageWrapper> {
    private String address;
    private int port;

    public ReqEndpoint(ZContext context) {
        super(new ZmqReqTransport(context.createSocket(SocketType.REQ)), new ProtobufCodec());
    }

    public void setLinger(int linger) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.setLinger(linger);
    }

    public void connectSocket(String address, int port) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);

        this.address = address;
        this.port = port;
    }

    public void disconnectSocket() {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.disconnect("tcp://" + address + ":" + port);
    }

    private ZmqReqTransport getTransportInternal() {
        return (ZmqReqTransport) super.getTransport();
    }
}
