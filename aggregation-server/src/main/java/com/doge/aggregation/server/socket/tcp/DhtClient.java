package com.doge.aggregation.server.socket.tcp;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.doge.common.codec.JsonCodec;
import com.doge.common.socket.Endpoint;
import com.doge.common.socket.tcp.TcpTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DhtClient extends Endpoint<JsonNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DhtClient(String host, int port) throws IOException {
        super(new TcpTransport(new Socket(host, port)), new JsonCodec());
    }

    public void create(String key, List<Integer> value) {
        JsonNode message = buildSetMessage(key, value);
        this.send(message);
    }

    private JsonNode buildSetMessage(String key, List<Integer> value) {
        return MAPPER.createObjectNode()
                .put("type", "set")
                .set("data", MAPPER.createObjectNode()
                        .put("key", key)
                        .set("value", MAPPER.valueToTree(value)));
    }
}
