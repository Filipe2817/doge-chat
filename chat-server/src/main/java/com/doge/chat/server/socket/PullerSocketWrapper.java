package com.doge.chat.server.socket;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.common.AbstractSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper;
import com.google.protobuf.InvalidProtocolBufferException;

public class PullerSocketWrapper extends AbstractSocketWrapper<ZMQ.Socket> {
    public PullerSocketWrapper(ZMQ.Socket socket) {
        super(socket);
    }
    
    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void sendMessage(String header, MessageWrapper wrapper) {}

    @Override
    public void receiveMessage() throws HandlerNotFoundException, InvalidProtocolBufferException {
        byte[] data = socket.recv(0);
        MessageWrapper wrapper = MessageWrapper.parseFrom(data);
        this.runHandler(wrapper.getMessageTypeCase(), wrapper);
    }

    public static ZMQ.Socket createSocket(ZContext context) {
        ZMQ.Socket socket = context.createSocket(SocketType.PULL);
        return socket;
    }

    public static void bindSocket(ZMQ.Socket socket, String address, int port) {
        socket.bind("tcp://" + address + ":" + port);
    }
}
