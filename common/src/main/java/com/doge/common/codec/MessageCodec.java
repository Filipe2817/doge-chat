package com.doge.common.codec;

import com.doge.common.exception.InvalidFormatException;

public interface MessageCodec<M> {
    byte[] encode(M message);

    M decode(byte[] data) throws InvalidFormatException;

    Object getMessageType(M message);
}
