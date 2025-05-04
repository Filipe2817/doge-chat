package com.doge.client;

import java.util.concurrent.Callable;

import org.zeromq.ZContext;

import com.doge.client.socket.zmq.PushEndpoint;
import com.doge.client.socket.zmq.ReqEndpoint;
import com.doge.client.socket.zmq.SubEndpoint;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "client", mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {
    @Option(names = {"-sc", "--server-chat"}, required = true, 
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

            PushEndpoint pushEndpoint = new PushEndpoint(context);
            pushEndpoint.connectSocket("localhost", this.chatServerPort);
            console.debug("Connected to PULL socket on port " + this.chatServerPort);
            
            ReqEndpoint reqEndpoint = new ReqEndpoint(context);
            reqEndpoint.connectSocket("localhost", (this.chatServerPort + 1));
            console.debug("Connected to REP socket on port " + (this.chatServerPort + 1));

            SubEndpoint subEndpoint = new SubEndpoint(context);
            subEndpoint.connectSocket("localhost", (this.chatServerPort + 2));
            console.debug("Connected to PUB socket on port " + (this.chatServerPort + 2));

            Client client;
            client = new Client(pushEndpoint, reqEndpoint, subEndpoint, console);

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
