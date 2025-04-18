package com.doge.common;

import com.doge.common.proto.MessageWrapper;

public interface Handler {
    void handle(MessageWrapper wrapper);
}
