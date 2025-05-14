package com.doge.client.socket.reactive;

import com.doge.client.Console;
import com.doge.common.proto.LogRequestMessage;
import com.doge.common.proto.Rx3LogServiceGrpc;
import com.doge.common.proto.UserLogRequestMessage;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class ReactiveGrpcClient {
    private final Console console;

    private Rx3LogServiceGrpc.RxLogServiceStub stub;

    public ReactiveGrpcClient(Console console) {
        this.console = console;
    }

    public void setup(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();

        this.stub = Rx3LogServiceGrpc.newRxStub(channel);
    }

    public void getLogs(String topic, int last) {
        LogRequestMessage request = LogRequestMessage.newBuilder()
            .setTopic(topic)
            .setLast(last)
            .build();

        stub.getLogs(Single.just(request))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io(), false, 32)
            .subscribe(
                resp ->  console.info("[LOGS] [" + resp.getClientId() + "] " + resp.getContent()),
                error -> console.error("[LOGS] Error: " + error.getMessage()),
                () -> console.info("[LOGS] Successfully received all logs!")
            );
    }

    public void getUserLogs(String topic, String userId, int last) {
        UserLogRequestMessage request = UserLogRequestMessage.newBuilder()
            .setTopic(topic)
            .setUserId(userId)
            .setLast(last)
            .build();

        stub.getUserLogs(Single.just(request))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io(), false, 32)
            .subscribe(
                resp -> console.info("[LOGS] [" + resp.getClientId() + "] " + resp.getContent()),
                error -> console.error("[LOGS] Error: " + error.getMessage()),
                () -> console.info("[LOGS] Successfully received all logs!")
            );
    }
}
