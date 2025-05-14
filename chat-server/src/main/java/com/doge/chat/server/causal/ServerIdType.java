package com.doge.chat.server.causal;

public @interface ServerIdType {
    // This annotation is used to mark the server Id type in the vector clock
    // implementation. It can be used for type safety and to indicate that the
    // annotated field or parameter should only accept server Ids
}
