package com.doge.client.socket.tcp;

import java.io.IOException;
import java.net.Socket;

import com.doge.common.codec.JsonCodec;
import com.doge.common.socket.Endpoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DhtClient extends Endpoint<JsonNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DhtClient(String host, int port) throws IOException {
        super(new TcpTransport(new Socket(host, port)), new JsonCodec());
    }

    // FIXME: Rethink the design of DHT requests
    // I don't like them being asynchronous
    public void asyncGet(String key) {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("type", "GET");
        request.put("key", key);
        
        this.send(request);
    }
}
