package com.doge.common.codec;

import com.doge.common.exception.InvalidFormatException;
import com.doge.common.proto.MessageWrapper;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtobufCodec implements MessageCodec<MessageWrapper> {
    @Override
    public byte[] encode(MessageWrapper message) {
        return message.toByteArray();    
    }

    @Override
    public MessageWrapper decode(byte[] data) throws InvalidFormatException {
        try {
            return MessageWrapper.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidFormatException("Failed to decode message: " + e.getMessage());
        }
    }

    @Override
    public Object getMessageType(MessageWrapper message) {
        return message.getMessageTypeCase();
    }
}
 