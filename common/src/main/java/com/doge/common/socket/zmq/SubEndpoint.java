package com.doge.common.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class SubEndpoint extends Endpoint<MessageWrapper> {
    private String address;
    private int port;

    public SubEndpoint(ZContext context) {
        super(new ZmqSubTransport(context.createSocket(SocketType.SUB)), new ProtobufCodec());
    }

    public void connectSocket(String address, int port) {
        ZmqSubTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);

        this.address = address;
        this.port = port;
    }

    public void disconnectSocket() {
        ZmqSubTransport transport = this.getTransportInternal();
        transport.socket.disconnect("tcp://" + address + ":" + port);
    }

    public void subscribe(String topic) {
        ZmqSubTransport transport = this.getTransportInternal();
        transport.socket.subscribe(topic.getBytes());
    }

    public void unsubscribe(String topic) {
        ZmqSubTransport transport = this.getTransportInternal();
        transport.socket.unsubscribe(topic.getBytes());
    }

    private ZmqSubTransport getTransportInternal() {
        return (ZmqSubTransport) super.getTransport();
    }
}
