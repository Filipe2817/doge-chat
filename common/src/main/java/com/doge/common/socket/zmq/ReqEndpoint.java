package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class ReqEndpoint extends Endpoint<MessageWrapper> {
    private String address;
    private int port;

    public ReqEndpoint(ZContext context) {
        super(new ZmqReqTransport(context.createSocket(SocketType.REQ)), new ProtobufCodec());
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

    public ZMQ.Socket getSocket() {
        ZmqReqTransport transport = this.getTransportInternal();
        return transport.socket;
    }
    
    public void setLinger(int linger) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.setLinger(linger);
    }

    public void setHeartbeatInterval(int ms) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.setHeartbeatIvl(ms);
    }

    public void setHeartbeatTimeout(int ms) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.setHeartbeatTimeout(ms);
    }

    public void setHeartbeatTTL(int ms) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.setHeartbeatTtl(ms);
    }

    private ZmqReqTransport getTransportInternal() {
        return (ZmqReqTransport) super.getTransport();
    }
}
