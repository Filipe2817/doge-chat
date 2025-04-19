package com.doge.chat.server.socket;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.common.AbstractSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper;
import com.google.protobuf.InvalidProtocolBufferException;

public class PublisherSocketWrapper extends AbstractSocketWrapper<ZMQ.Socket> {
    public PublisherSocketWrapper(ZMQ.Socket socket) {
        super(socket);
    }

    @Override
    public void close() {
        socket.close();
    }

    @Override
    public void sendMessage(String header, MessageWrapper wrapper) {
        socket.sendMore(header);
        socket.send(wrapper.toByteArray());
    }

    @Override
    public void receiveMessage() throws HandlerNotFoundException, InvalidProtocolBufferException {}

    public static ZMQ.Socket createSocket(ZContext context) {
        ZMQ.Socket socket = context.createSocket(SocketType.PUB);
        return socket;
    }

    public static void bindSocket(ZMQ.Socket socket, String address, int port) {
        socket.bind("tcp://" + address + ":" + port);
    }
}
