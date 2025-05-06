package com.doge.aggregation.server;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.zeromq.ZContext;

import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;
import com.doge.aggregation.server.AggregationServer;
import com.doge.aggregation.server.neighbours.NeighbourManager;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
    @Option(names = "-p",
        description = """
        First port for the aggregation server to listen on.
        PULL will use this port.
        PUSH will use this port + 1 for asynchronous client requests.
        """,
        defaultValue = "6666"
    )
    private int port = 6666;
    private int cacheSize = 5;


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
            logger.debug("ROUTER socket bound to port " + pullPort);

            NeighbourManager neighbourManager = new NeighbourManager(
                this.cacheSize,
                logger,
                pullEndpoint,
                context
            );

            AggregationServer aggregationServer = new AggregationServer(
                this.port,
                this.cacheSize,
                this.cacheSize,
                pullEndpoint,
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