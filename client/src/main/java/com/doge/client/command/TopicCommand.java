package com.doge.client.command;

import java.util.List;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.exception.KeyNotFoundException;
import com.doge.client.socket.tcp.DhtClient;
import com.doge.common.proto.AggregationResultMessage;
import com.doge.common.proto.AggregationStartMessage;
import com.doge.common.proto.ExitMessage;
import com.doge.common.proto.MessageWrapper;
import com.doge.common.socket.zmq.PushEndpoint;
import com.doge.common.socket.zmq.ReqEndpoint;

public class TopicCommand extends AbstractCommand {
    private Client client;

    private PushEndpoint pushEndpoint;
    private ReqEndpoint aggregationServerReqEndpoint;
    private DhtClient dhtClient;

    public TopicCommand(
        Client client,
        PushEndpoint pushEndpoint,
        ReqEndpoint aggregationServerReqEndpoint,
        DhtClient dhtClient
    ) {
        super("/topic", "<topic>");
        this.client = client;

        this.pushEndpoint = pushEndpoint;
        this.aggregationServerReqEndpoint = aggregationServerReqEndpoint;
        this.dhtClient = dhtClient; 
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length != 1) {
            sendUsage(console);
            return;
        }

        String topic = args[0];

        console.info("Searching for chat servers serving topic '" + topic + "' in DHT...");
        int chosenChatServer = this.searchByTopic(console, topic);

        if (chosenChatServer == -1) {
            console.info("Starting aggregation...");

            chosenChatServer = this.startAggregation(console, topic);
            if (chosenChatServer == -1) {
                console.error("Failed to aggregate. Try again later...");
                return;
            }
        }

        MessageWrapper exitMessage = createExitMessage(client.getCurrentTopic(), client.getId());
        this.pushEndpoint.send(exitMessage);
        console.info("Notified old chat server about topic change");

        client.setCurrentChatServer(chosenChatServer);
        client.setCurrentTopic(topic);

        console.info("Setting up connections to chosen chat server...");
        client.resetConnectionsToChatServer();
    }

    private int searchByTopic(Console console, String topic) {
        try {
            List<Integer> chatServers = this.dhtClient.search(topic);
            if (chatServers.isEmpty()) {
                console.error("No chat servers found for topic '" + topic + "'");
                return -1;
            }

            int chosenChatServer = this.client.updateChatServers(chatServers);

            console.info("Found chat server with id '" + chosenChatServer + "' for topic '" + topic + "'");
            return chosenChatServer;
        } catch (KeyNotFoundException e) {
            console.error("No chat server found for topic '" + topic + "'");
            return -1;
        } catch (Exception e) {
            console.error("[DHT] Error while searching by topic: " + e.getMessage());
            return -1;
        }
    }

    private int startAggregation(Console console, String topic) {
        String clientId = this.client.getId();
        int aggregationCount = this.client.getAggregationCount();
        String dotClientId = clientId + "-" + aggregationCount;
        this.client.incrementAggregationCount();

        AggregationStartMessage startAggregationMessage = AggregationStartMessage.newBuilder()
                .setDotClientId(dotClientId)
                .setTopic(topic)
                .build();

        MessageWrapper messageWrapper = MessageWrapper.newBuilder()
                .setAggregationStartMessage(startAggregationMessage)
                .build();

        this.aggregationServerReqEndpoint.send(messageWrapper);
        console.debug("[REQ | Aggregation server] Sent aggregation start message");

        try {
            MessageWrapper response = this.aggregationServerReqEndpoint.receiveOnceWithoutHandle();
            AggregationResultMessage result = response.getAggregationResultMessage();
            List<Integer> chatServers = result.getServerIdsList();
            console.warn("Got response from aggregation server. Found C chat servers: " + chatServers);

            int chosenChatServer = client.updateChatServers(chatServers);

            console.info("Chose chat server with id '" + chosenChatServer + "' for topic '" + topic + "'");
            return chosenChatServer;
        } catch (Exception e) {
            console.error("[REQ | Aggregation server] Error while receiving aggregation result: " + e.getMessage());
            return -1;
        }
    }

    private MessageWrapper createExitMessage(String topic, String clientId) {
        ExitMessage exitMessage = ExitMessage.newBuilder()
                .setTopic(topic)
                .setClientId(clientId)
                .build();

        return MessageWrapper.newBuilder()
                .setExitMessage(exitMessage)
                .build();
    }
}
