package com.doge.aggregation.server;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.aggregation.server.socket.zmq.PushEndpoint;
import com.doge.common.Logger;
import com.doge.aggregation.server.neighbours.Neighbour;
import com.doge.aggregation.server.neighbours.NeighbourManager;
import com.doge.aggregation.server.socket.zmq.PullEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
    @Option(names = "-p",
        description = """
        First port for the aggregation server to listen on.
        PULL will use this port.
        """,
        defaultValue = "6666"
    )
    private int port = 6666;

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

            NeighbourManager neighbourManager = new NeighbourManager(this.c);
            
            // Create introduction node, if provided
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
