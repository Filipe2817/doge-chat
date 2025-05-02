package com.doge.chat.server;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.zeromq.ZContext;

import com.doge.chat.server.causal.VectorClockManager;
import com.doge.chat.server.socket.zmq.PubEndpoint;
import com.doge.chat.server.socket.zmq.PullEndpoint;
import com.doge.chat.server.socket.zmq.RepEndpoint;
import com.doge.chat.server.socket.zmq.SubEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "chat-server", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = "-t", description = "Topic to handle", defaultValue = "default")
    private String topic = "default";

    @Option(names = "-p",
        description = """
        First port for the chat server to listen on.
        PULL will use this port.
        REQ will use this port + 1 for synchronous client requests.
        PUB will use this port + 2 for clients.
        PUB will use this port + 3 for other chat servers.
        """,
        defaultValue = "5555"
    )
    private int port = 5555;

    @Option(names = {"-sc", "--subscriber-ports"}, split = ",", required = true, description = "Comma-separated list of ports for chat servers to connect to")
    private List<Integer> chatServerPorts;

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
            logger.debug("PULL socket bound to port " + pullPort);
            
            SubEndpoint subEndpoint = new SubEndpoint(context);
            for (Integer port : this.chatServerPorts) {
                subEndpoint.connectSocket("localhost", port);
                logger.debug("SUB socket connected to port " + port);
            }
            
            int repPort = this.port + 1;
            RepEndpoint repEndpoint = new RepEndpoint(context);
            repEndpoint.bindSocket("localhost", repPort);
            logger.debug("REP socket bound to port " + repPort);

            int clientPubPort = this.port + 2;
            PubEndpoint clientPubEndpoint = new PubEndpoint(context);
            clientPubEndpoint.bindSocket("localhost", clientPubPort);
            logger.debug("Client PUB socket bound to port " + clientPubPort);

            int chatServerPubPort = this.port + 3;
            PubEndpoint chatServerPubEndpoint = new PubEndpoint(context);
            chatServerPubEndpoint.bindSocket("localhost", chatServerPubPort);
            logger.debug("ChatServer PUB socket bound to port " + chatServerPubPort);

            VectorClockManager vectorClockManager = new VectorClockManager(this.port);
            List<Integer> chatServerPorts = this.chatServerPorts.stream()
                .map(port -> port - 3)
                .collect(Collectors.toList());
            chatServerPorts.add(this.port);
            vectorClockManager.addTopic(topic, chatServerPorts);
            logger.debug("Vector clock manager initialized for topic " + "'" + topic + "' with identifiers " + chatServerPorts);

            ChatServer chatServer = new ChatServer(
                this.port,
                this.topic,
                pullEndpoint,
                subEndpoint,
                repEndpoint,
                clientPubEndpoint,
                chatServerPubEndpoint,
                vectorClockManager,
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
