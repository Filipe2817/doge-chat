package com.doge.client.socket.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import com.doge.common.codec.ProtobufCodec;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.Endpoint;

public class ReqEndpoint extends Endpoint<MessageWrapper> {
    public ReqEndpoint(ZContext context) {
        super(new ZmqReqTransport(context.createSocket(SocketType.REQ)), new ProtobufCodec());
    }

    public MessageWrapper receiveOnceWithoutHandle() throws InvalidFormatException {
        ZmqReqTransport transport = this.getTransportInternal();
        ProtobufCodec codec = this.getCodecInternal();

        byte[] rawMessage = transport.receive();
        MessageWrapper message = codec.decode(rawMessage);

        return message;
    }

    public void connectSocket(String address, int port) {
        ZmqReqTransport transport = this.getTransportInternal();
        transport.socket.connect("tcp://" + address + ":" + port);
    }

    private ZmqReqTransport getTransportInternal() {
        return (ZmqReqTransport) super.getTransport();
    }

    private ProtobufCodec getCodecInternal() {
        return (ProtobufCodec) super.getCodec();
    }
}
