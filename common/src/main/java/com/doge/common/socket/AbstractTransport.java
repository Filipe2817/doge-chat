package com.doge.common.socket;

public abstract class AbstractTransport {
    public abstract void close();

    /**
     * Send with header and payload - subclasses *must* implement
     */
    public abstract void send(String header, byte[] data);

    /**
     * Convenience overload for transports that ignore the header
     */
    public void send(byte[] data) {
        send("", data);
    }

    public abstract byte[] receive();
}
