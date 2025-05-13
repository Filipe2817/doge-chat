package com.doge.client;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.client.socket.reactive.ReactiveGrpcClient;
import com.doge.client.socket.zmq.PushEndpoint;
import com.doge.client.socket.zmq.ReqEndpoint;
import com.doge.client.socket.zmq.SubEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "client", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = {"-n", "--name"}, required = true, description = "Name of the client to be used as an identifier.")
    private String name;

    @Option(names = {"-cs", "--chat-server"}, required = true, 
        description = """
        Port for chat server to connect to.
        PUSH will use this port.
        REQ will use this port + 1.
        SUB will use this port + 2.
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

            int pullPort = this.chatServerPort;
            PushEndpoint pushEndpoint = new PushEndpoint(context);
            pushEndpoint.connectSocket("localhost", pullPort);
            console.debug("[PULL] Connected on port " + pullPort);
            
            int repPort = this.chatServerPort + 1;
            ReqEndpoint reqEndpoint = new ReqEndpoint(context);
            reqEndpoint.connectSocket("localhost", repPort);
            console.debug("[REQ] Connected on port " + repPort);

            int pubPort = this.chatServerPort + 2;
            SubEndpoint subEndpoint = new SubEndpoint(context);
            subEndpoint.connectSocket("localhost", pubPort);
            console.debug("[SUB] Connected on port " + pubPort);

            int reactivePort = this.chatServerPort + 4;
            ReactiveGrpcClient reactiveClient = new ReactiveGrpcClient(console);
            reactiveClient.setup("localhost", reactivePort);
            console.debug("[REACTIVE] Connected on port " + reactivePort);

            Client client = new Client(name, pushEndpoint, reqEndpoint, subEndpoint, reactiveClient, console);
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
