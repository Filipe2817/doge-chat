package com.doge.chat.server.socket.reactive;

import java.util.List;

import com.doge.chat.server.log.LogManager;
import com.doge.chat.server.log.LogMessage;
import com.doge.common.Logger;
import com.doge.common.proto.ChatMessage;
import com.doge.common.proto.LogRequestMessage;
import com.doge.common.proto.Rx3LogServiceGrpc;
import com.doge.common.proto.UserLogRequestMessage;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

public class ReactiveLogService extends Rx3LogServiceGrpc.LogServiceImplBase {
    private final Logger logger;
    private final LogManager logManager;

    public ReactiveLogService(Logger logger, LogManager logManager) {
        this.logger = logger;
        this.logManager = logManager;
    }

    @Override
    public Flowable<ChatMessage> getLogs(Single<LogRequestMessage> request) {
        return request.flatMapPublisher(req -> {
            // Without a call to `defer`, the `snapshot()` is created
            // when the `getLogs` is invoked and not when the client
            // is ready to consume from the stream
            //
            // This way, we maximize for having a strong consistency
            // regarding the logs each client receives
            return Flowable.defer(() -> {
                List<LogMessage> snapshot = this.logManager.snapshot();

                return Flowable
                    .fromIterable(snapshot)
                    .map(log -> log.chatMessage())
                    .filter(log -> log.getTopic().equals(req.getTopic()))
                    .takeLast(req.getLast());
            })
            .doOnComplete(() -> logger.debug("Completed log request for topic '" + req.getTopic() + "'"));
        });
    } 

    @Override
    public Flowable<ChatMessage> getUserLogs(Single<UserLogRequestMessage> request) {
        return request.flatMapPublisher(req -> {
            // Refer to above comment
            return Flowable.defer(() -> {
                List<LogMessage> snapshot = this.logManager.snapshot();

                return Flowable
                    .fromIterable(snapshot)
                    .map(log -> log.chatMessage())
                    .filter(log -> log.getClientId().equals(req.getUserId()))
                    .filter(log -> log.getTopic().equals(req.getTopic()))
                    .takeLast(req.getLast());
            })
            .doOnComplete(() -> logger.debug("Completed log request for user '" + req.getUserId() + "' and topic '" + req.getTopic() + "'"));
        });
    }
}
