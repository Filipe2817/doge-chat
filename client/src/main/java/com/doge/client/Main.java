package com.doge.client;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.doge.client.socket.PusherSocketWrapper;
import com.doge.client.socket.SubscriberSocketWrapper;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "client", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = {"-sc", "--server-chat"}, required = true, 
        description = """
        Port for chat server to connect to.
        Pusher will use this port.
        Subscriber will use this port + 1.
        """, 
        defaultValue = "5555"
    )
    private int chatServerPort = 5555;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        ZContext context = null;
        try {
            context = new ZContext();

            ZMQ.Socket pusher = PusherSocketWrapper.createSocket(context);
            PusherSocketWrapper.connectSocket(pusher, "localhost", this.chatServerPort);
            PusherSocketWrapper pusherWrapper = new PusherSocketWrapper(pusher);

            ZMQ.Socket subscriber = SubscriberSocketWrapper.createSocket(context);
            SubscriberSocketWrapper.connectSocket(subscriber, "localhost", this.chatServerPort + 1);
            SubscriberSocketWrapper subscriberWrapper = new SubscriberSocketWrapper(subscriber);

            Client client = new Client(pusherWrapper, subscriberWrapper);
            client.run();

            return 0;
        } catch (Exception e) {
            System.err.println("Error running client: " + e.getMessage());
            return 1;
        } finally {
            context.close();
        }
    }
}
