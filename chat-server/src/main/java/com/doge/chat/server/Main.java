package com.doge.chat.server;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.log.LogManager;
import com.doge.chat.server.socket.reactive.ReactiveGrpcEndpoint;
import com.doge.chat.server.socket.reactive.ReactiveLogService;
import com.doge.chat.server.user.UserManager;
import com.doge.common.Logger;
import com.doge.common.socket.zmq.PubEndpoint;
import com.doge.common.socket.zmq.PullEndpoint;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.SubEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "chat-server", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = "-p",
        description = """
        First port for the chat server to listen on.
        PULL will use this port.
        REP will use this port + 1 for synchronous client and
        aggregation server requests.
        PUB will use this port + 2 for clients.
        PUB will use this port + 3 for other chat servers.
        REACTIVE will use this port + 4 for reactive gRPC.
        """,
        defaultValue = "5555"
    )
    private int port = 5555;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        ZContext context = null;
        try {
            Logger logger = new Logger();
            context = new ZContext();

            int pullPort = this.port;
            PullEndpoint pullEndpoint = new PullEndpoint(context);
            pullEndpoint.bindSocket("localhost", pullPort);
            logger.debug("[PULL] Bound to port " + pullPort);

            int repPort = this.port + 1;
            RepEndpoint repEndpoint = new RepEndpoint(context);
            repEndpoint.bindSocket("localhost", repPort);
            logger.debug("[REP] Bound to port " + repPort);

            int clientPubPort = this.port + 2;
            PubEndpoint clientPubEndpoint = new PubEndpoint(context);
            clientPubEndpoint.bindSocket("localhost", clientPubPort);
            logger.debug("[PUB | Client] bound to port " + clientPubPort);

            int chatServerPubPort = this.port + 3;
            PubEndpoint chatServerPubEndpoint = new PubEndpoint(context);
            chatServerPubEndpoint.bindSocket("localhost", chatServerPubPort);
            logger.debug("[PUB | Chat server] Bound to port " + chatServerPubPort);
            
            int reactivePort = this.port + 4;
            LogManager logManager = new LogManager();
            ReactiveLogService logService = new ReactiveLogService(logger, logManager);
            ReactiveGrpcEndpoint reactiveEndpoint = new ReactiveGrpcEndpoint(reactivePort, logService);
            logger.debug("[REACTIVE] Initialized on port " + reactivePort);

            SubEndpoint subEndpoint = new SubEndpoint(context);
            logger.debug("[SUB] Ready to connect to other chat servers");

            VectorClockManager vectorClockManager = new VectorClockManager(this.port);
            logger.debug("Vector clock manager initialized for port " + this.port);

            UserManager userManager = new UserManager(this.port);
            logger.debug("User manager initialized for port " + this.port);

            ChatServer chatServer = new ChatServer(
                this.port,
                pullEndpoint,
                subEndpoint,
                repEndpoint,
                clientPubEndpoint,
                chatServerPubEndpoint,
                reactiveEndpoint,
                logManager,
                vectorClockManager,
                userManager,
                logger
            );
            chatServer.run();

            return 0;
        } catch (Exception e) {
            System.err.println("Error running chat server: " + e.getMessage());
            return 1;
        } finally {
            context.close();
        }
    }
}
