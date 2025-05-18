package com.doge.aggregation.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.aggregation.server.neighbour.Neighbour;
import com.doge.aggregation.server.neighbour.NeighbourManager;
import com.doge.aggregation.server.socket.tcp.DhtClient;
import com.doge.common.InetSocketAddressConverter;
import com.doge.common.Logger;
import com.doge.common.socket.zmq.PullEndpoint;
import com.doge.common.socket.zmq.PushEndpoint;
import com.doge.common.socket.zmq.RepEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
    @Option(names = "-p",
        description = """
        First port for the aggregation server to listen on.
        PULL will use this port.
        REP will use this port + 1 for synchronous client requests.
        """,
        defaultValue = "6666"
    )
    private int port = 6666;

    @Option(names = "-cs",
        description = """
        Port for the chat server corresponding to this aggregation server.
        REQ will use this port + 1 for requesting to its chat server.
        """,
        defaultValue = "5555"
    )
    private int chatServerPort = 5555;

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

    @Option(names = "-c",
        description = "Size of the view for neighbours.",
        defaultValue = "5"
    )
    private int c = 5;

    @Option(names = "-l",
        description = "Number of peer-entries to send in the shuffle message.",
        defaultValue = "3"
    )
    private int l = 3;

    @Option(names = "-i", 
        description = "Id of the introduction server for an initial CYCLON.", 
        defaultValue = "0"
    )
    private int introId;

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

            int reqPort = this.chatServerPort + 1;
            ReqEndpoint reqEndpoint = new ReqEndpoint(context);
            reqEndpoint.connectSocket("localhost", reqPort);
            logger.debug("[REQ] Connected to port " + reqPort);

            int repPort = this.port + 1;
            RepEndpoint repEndpoint = new RepEndpoint(context);
            repEndpoint.bindSocket("localhost", repPort);
            logger.debug("[REP] Bound to port " + repPort);

            DhtClient dhtClient = new DhtClient(dhtNode.getHostString(), dhtNode.getPort());
            logger.debug("[DHT] Connected to " + dhtNode);

            NeighbourManager neighbourManager = new NeighbourManager(this.c);
            
            // Create introduction neighbour, if provided
            if (this.introId != 0) {
                PushEndpoint pushEndpoint = new PushEndpoint(context);
                pushEndpoint.connectSocket("localhost", introId);
                Neighbour introductionNode = new Neighbour(introId, pushEndpoint, 0, logger);
                neighbourManager.addNeighbour(introductionNode);
            }

            AggregationServer aggregationServer = new AggregationServer(
                this.port,
                this.l,
                context,
                pullEndpoint,
                repEndpoint,
                reqEndpoint,
                dhtClient,
                neighbourManager,
                logger
            );
            aggregationServer.run();

            return 0;
        } catch (Exception e) {
            System.err.println("Error running aggregation server: " + e.getMessage());
            return 1;
        } finally {
            context.close();
        }
    }
}
