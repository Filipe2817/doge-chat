package com.doge.client.socket;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.common.AbstractSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper;
import com.google.protobuf.InvalidProtocolBufferException;

public class PusherSocketWrapper extends AbstractSocketWrapper<ZMQ.Socket> {
    public PusherSocketWrapper(ZMQ.Socket socket) {
        super(socket);
    }
    
    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void sendMessage(String header, MessageWrapper wrapper) {
        this.socket.send(wrapper.toByteArray());
    }

    @Override
    public void receiveMessage() throws HandlerNotFoundException, InvalidProtocolBufferException {}

    public static ZMQ.Socket createSocket(ZContext context) {
        ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
        return socket;
    }
    
    public static void connectSocket(ZMQ.Socket socket, String address, int port) {
        socket.connect("tcp://" + address + ":" + port);
    }
}
