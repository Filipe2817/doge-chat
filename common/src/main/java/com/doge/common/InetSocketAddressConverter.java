package com.doge.common;

import java.net.InetSocketAddress;

import picocli.CommandLine.ITypeConverter;

public class InetSocketAddressConverter implements ITypeConverter<InetSocketAddress> {
    @Override
    public InetSocketAddress convert(String value) {
        String[] parts = value.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Expected HOST:PORT, got: " + value);
        }

        String host = parts[0];
        int port;

        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: " + parts[1], e);
        }

        return new InetSocketAddress(host, port);
    }
}
