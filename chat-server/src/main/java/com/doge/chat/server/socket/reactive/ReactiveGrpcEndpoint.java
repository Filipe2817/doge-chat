package com.doge.chat.server.socket.reactive;

import java.io.IOException;

import io.grpc.ServerBuilder;

public class ReactiveGrpcEndpoint {
    private final int port;

    private final ReactiveLogService logService;

    public ReactiveGrpcEndpoint(int port, ReactiveLogService logService) {
        this.port = port;

        this.logService = logService;
    }

    public void runServer() throws InterruptedException, IOException {
        ServerBuilder
            .forPort(this.port)
            .addService(this.logService)
            .build()
            .start()
            .awaitTermination();
    }
}
