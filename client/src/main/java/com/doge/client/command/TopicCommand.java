package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.SubEndpoint;

public class TopicCommand extends AbstractCommand {
    private final Client client;
    private final SubEndpoint subEndpoint;

    public TopicCommand(Client client, SubEndpoint subEndpoint) {
        super("/topic", "<topic>");
        this.client = client;
        this.subEndpoint = subEndpoint;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length < 1) {
            sendUsage(console);
            return;
        }

        String topic = args[0];

        // FIXME: Changing topic is a much more complex operation
        // this is just for testing purposes
        subEndpoint.unsubscribe(client.getCurrentTopic());
        client.setCurrentTopic(topic);
        subEndpoint.subscribe(topic);
        console.info("Subscribed to topic: " + topic);
    }
}
