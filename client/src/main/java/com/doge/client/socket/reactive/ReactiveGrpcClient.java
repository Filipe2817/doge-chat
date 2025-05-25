package com.doge.client.socket.reactive;

import java.util.concurrent.TimeUnit;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.common.proto.LogRequestMessage;
import com.doge.common.proto.Rx3LogServiceGrpc;
import com.doge.common.proto.UserLogRequestMessage;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class ReactiveGrpcClient {
    private Client client;
    private final Console console;

    private Rx3LogServiceGrpc.RxLogServiceStub stub;
    private ManagedChannel channel;
    private Disposable currentSubscription;

    public ReactiveGrpcClient(Client client, Console console) {
        this.client = client;
        this.console = console;
    }

    public void setup(String host, int port) {
        this.channel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();

        this.stub = Rx3LogServiceGrpc.newRxStub(channel);
    }

    public void close() {
        if (currentSubscription != null && !currentSubscription.isDisposed()) {
            currentSubscription.dispose();
            console.info("[LOGS] Subscription disposed");
        }

        if (channel != null) {
            channel.shutdown();

            try {
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }

            console.info("[LOGS] Channel shut down");
        }
    }

    public void cancel() {
        if (currentSubscription != null && !currentSubscription.isDisposed()) {
            currentSubscription.dispose();
        }
    }

    public void getLogs(String topic, int last) {
        this.client.rpcStarted();

        LogRequestMessage request = LogRequestMessage.newBuilder()
            .setTopic(topic)
            .setLast(last)
            .build();

        currentSubscription = stub.getLogs(Single.just(request))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io(), false, 32)
            .subscribe(
                resp -> console.info("[LOGS] [" + resp.getClientId() + "] " + resp.getContent()),
                error -> console.error("[LOGS] Error: " + error.getMessage()),
                () -> {
                    console.info("[LOGS] Successfully received all logs");
                    this.client.rpcDone();
                }
            );
    }

    public void getUserLogs(String topic, String userId, int last) {
        this.client.rpcStarted();

        UserLogRequestMessage request = UserLogRequestMessage.newBuilder()
            .setTopic(topic)
            .setUserId(userId)
            .setLast(last)
            .build();

        currentSubscription = stub.getUserLogs(Single.just(request))
            .subscribeOn(Schedulers.io())
            .observeOn(Schedulers.io(), false, 32)
            .subscribe(
                resp -> console.info("[LOGS] [" + resp.getClientId() + "] " + resp.getContent()),
                error -> console.error("[LOGS] Error: " + error.getMessage()),
                () -> {
                    console.info("[LOGS] Successfully received all logs");
                    this.client.rpcDone();
                }
            );
    }
}
