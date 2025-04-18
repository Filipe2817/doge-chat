package com.doge.client.command;

import com.doge.client.Client;
import com.doge.client.Console;
import com.doge.client.socket.SubscriberSocketWrapper;

public class TopicCommand extends AbstractCommand {
    private final Client client;
    private final SubscriberSocketWrapper subscriberWrapper;

    public TopicCommand(Client client, SubscriberSocketWrapper subscriberWrapper) {
        super("/topic", "<topic>");
        this.client = client;
        this.subscriberWrapper = subscriberWrapper;
    }

    @Override
    public void execute(Console console, String[] args) {
        if (args.length < 1) {
            sendUsage(console);
            return;
        }

        String topic = args[0];

        subscriberWrapper.unsubscribe(client.getCurrentTopic());
        client.setCurrentTopic(topic);
        subscriberWrapper.subscribe(topic);
        console.info("Subscribed to topic: " + topic);
    }
}
