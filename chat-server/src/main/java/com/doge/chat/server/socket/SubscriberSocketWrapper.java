package com.doge.chat.server.socket;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.common.AbstractSocketWrapper;
import com.doge.common.exception.HandlerNotFoundException;
import com.doge.common.proto.MessageWrapper;
import com.google.protobuf.InvalidProtocolBufferException;

public class SubscriberSocketWrapper extends AbstractSocketWrapper<ZMQ.Socket> {
    public SubscriberSocketWrapper(ZMQ.Socket socket) {
        super(socket);
    }
    
    @Override
    public void close() {
        this.socket.close();
    }

    @Override
    public void sendMessage(String header, MessageWrapper wrapper) {
        this.socket.sendMore(header);
        this.socket.send(wrapper.toByteArray());
    }

    @Override
    public void receiveMessage() throws HandlerNotFoundException, InvalidProtocolBufferException {
        socket.recvStr();
        byte[] data = socket.recv(0);
        MessageWrapper wrapper = MessageWrapper.parseFrom(data);
        this.runHandler(wrapper.getMessageTypeCase(), wrapper);
    }

    public void subscribe(String topic) {
        socket.subscribe(topic.getBytes(ZMQ.CHARSET));
    }

    public void unsubscribe(String topic) {
        socket.unsubscribe(topic.getBytes(ZMQ.CHARSET));
    }

    public static ZMQ.Socket createSocket(ZContext context) {
        ZMQ.Socket socket = context.createSocket(SocketType.SUB);
        return socket;
    }

    public static void connectSocket(ZMQ.Socket socket, String address, int port) {
        socket.connect("tcp://" + address + ":" + port);
    }
}
