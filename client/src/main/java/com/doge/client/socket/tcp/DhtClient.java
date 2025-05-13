package com.doge.client.socket.tcp;

import java.io.IOException;
import java.net.Socket;

import com.doge.common.codec.JsonCodec;
import com.doge.common.socket.Endpoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DhtClient extends Endpoint<JsonNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public DhtClient(String host, int port) throws IOException {
        super(new TcpTransport(new Socket(host, port)), new JsonCodec());
    }
}
