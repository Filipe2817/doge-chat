package com.doge.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.common.InetSocketAddressConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "client", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = {"-n", "--name"}, required = true, description = "Name of the client to be used as an identifier.")
    private String name;

    @Option(names = {"-t", "--topic"}, 
        description = """
        Topic to create or join.
        Can be later changed using the command /topic.
        """,
        defaultValue = "default"
    )
    private String topic = "default";

    @Option(names = "-dht", required = true,
        paramLabel = "HOST:PORT",
        description = """
        Host and port of the DHT node to connect to.
        Examples: 
        - 192.168.1.5:7888;
        - localhost:4000;
        - dht.doge.com:5555.
        """,
        defaultValue = "127.0.0.1:8000",
        converter = InetSocketAddressConverter.class
    )
    private InetSocketAddress dhtNode;

    @Option(names = "-as", 
        description = """
        Id of the aggregation server to connect to.
        Aggregation servers are used for aggregating chat
        servers whenever a new topic is created.
        """,
        required = true
    )
    private int aggregationServerId;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        ZContext context = null;
        try {
            context = new ZContext();

            Client client = new Client(
                name, 
                topic,
                dhtNode,
                aggregationServerId,
                context
            );
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
