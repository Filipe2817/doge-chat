package com.doge.client.socket.tcp;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.doge.client.exception.KeyNotFoundException;
import com.doge.common.codec.JsonCodec;
import com.doge.common.exception.InvalidFormatException;
import com.doge.common.socket.Endpoint;
import com.doge.common.socket.tcp.TcpTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DhtClient extends Endpoint<JsonNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DhtClient(String host, int port) throws IOException {
        super(new TcpTransport(new Socket(host, port)), new JsonCodec());
    }

    public List<Integer> search(String key) throws InvalidFormatException, KeyNotFoundException {
        JsonNode message = buildGetMessage(key);
        this.send(message);

        JsonNode response = this.receiveOnceWithoutHandle();
        if (response.get("status").asText().equals("ok")) {
            JsonNode data = response.get("data");
            JsonNode values = data.get("value");

            return MAPPER.convertValue(
                values,
                new TypeReference<List<Integer>>() {}
            );
        } else {
            throw new KeyNotFoundException("Key not found: " + key);
        }
    }

    private JsonNode buildGetMessage(String key) {
        return MAPPER.createObjectNode()
                .put("type", "get")
                .set("data", MAPPER.createObjectNode().put("key", key));
    }
}
