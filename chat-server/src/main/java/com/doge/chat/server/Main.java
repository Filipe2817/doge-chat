package com.doge.chat.server;

import java.util.List;
import java.util.concurrent.Callable;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.chat.server.socket.PublisherSocketWrapper;
import com.doge.chat.server.socket.PullerSocketWrapper;
import com.doge.chat.server.socket.SubscriberSocketWrapper;

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
        Puller will use this port.
        Publisher will use this port + 1 for clients.
        Publisher will use this port + 2 for other chat servers.
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
            context = new ZContext();

            ZMQ.Socket puller = PullerSocketWrapper.createSocket(context);
            PullerSocketWrapper.bindSocket(puller, "localhost", this.port);
            System.out.println("Puller bound to port " + this.port);
            PullerSocketWrapper pullerWrapper = new PullerSocketWrapper(puller);

            ZMQ.Socket subscriber = SubscriberSocketWrapper.createSocket(context);
            for (Integer port : this.chatServerPorts) {
                SubscriberSocketWrapper.connectSocket(subscriber, "localhost", port);
                System.out.println("Subscriber connected to port " + port);
            }
            SubscriberSocketWrapper subscriberWrapper = new SubscriberSocketWrapper(subscriber);

            ZMQ.Socket clientPublisher = PublisherSocketWrapper.createSocket(context);
            PublisherSocketWrapper.bindSocket(clientPublisher, "localhost", this.port + 1);
            System.out.println("Client publisher bound to port " + (this.port + 1));
            PublisherSocketWrapper clientPublisherWrapper = new PublisherSocketWrapper(clientPublisher);

            ZMQ.Socket chatServerPublisher = PublisherSocketWrapper.createSocket(context);
            PublisherSocketWrapper.bindSocket(chatServerPublisher, "localhost", this.port + 2);
            System.out.println("Chat server publisher bound to port " + (this.port + 2));
            PublisherSocketWrapper chatServerPublisherWrapper = new PublisherSocketWrapper(chatServerPublisher);

            ChatServer chatServer = new ChatServer(
                topic,
                pullerWrapper,
                subscriberWrapper,
                clientPublisherWrapper,
                chatServerPublisherWrapper
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
