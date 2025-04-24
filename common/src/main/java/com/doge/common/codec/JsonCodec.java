package com.doge.common.codec;

import java.io.IOException;

import com.doge.common.exception.InvalidFormatException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonCodec implements MessageCodec<JsonNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String key;

    public JsonCodec() {
        this("type");
    }

    public JsonCodec(String key) {
        this.key = key;
    }

    @Override
    public byte[] encode(JsonNode message) {
        try {
            return MAPPER.writeValueAsBytes(message);
        } catch (JsonProcessingException ignored) {
            return null;
        }
    }

    @Override
    public JsonNode decode(byte[] data) throws InvalidFormatException {
        try {
            return MAPPER.readTree(data);
        } catch (IOException e) {
            throw new InvalidFormatException("Invalid JSON format", e);
        }
    }

    @Override
    public Object getMessageType(JsonNode message) {
        JsonNode node = message.get(key);
        return node == null ? null : (node.isTextual() ? node.asText() : node);
    }
}
