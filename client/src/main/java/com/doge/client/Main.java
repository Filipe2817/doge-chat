package com.doge.client;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.client.socket.DhtClient;
import com.doge.client.socket.PushEndpoint;
import com.doge.client.socket.SubEndpoint;

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
            Console console = new Console();
            context = new ZContext();

            PushEndpoint pushEndpoint = new PushEndpoint(context);
            pushEndpoint.connectSocket("localhost", this.chatServerPort);
            console.debug("Connected to PULL socket on port " + this.chatServerPort + " for sending messages to chat server");
            System.out.println("Connect to chat server PULL socket on port " + this.chatServerPort);

            SubEndpoint subEndpoint = new SubEndpoint(context);
            subEndpoint.connectSocket("localhost", this.chatServerPort + 1);
            console.debug("Connected to PUB socket on port " + (this.chatServerPort + 1) + " for receiving messages from chat server");

            Client client;
            try {
                DhtClient dhtClient = new DhtClient("localhost", 9000);
                console.debug("Connected to DHT on port 9000");
                client = new Client(pushEndpoint, subEndpoint, dhtClient, console);
            } catch (Exception e) {
                console.debug("Failed to connect to DHT: " + e.getMessage());
                client = new Client(pushEndpoint, subEndpoint, null, console);
            }

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
