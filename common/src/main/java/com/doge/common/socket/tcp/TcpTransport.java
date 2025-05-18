package com.doge.common.socket.tcp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;

import com.doge.common.socket.AbstractTransport;

public class TcpTransport extends AbstractTransport {
    protected final Socket socket;

    private final DataInputStream in;
    private final DataOutputStream out;

    public TcpTransport(Socket socket) {
        this.socket = socket;

        try {
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create TcpTransport class", e);
        }
    }

    @Override
    public void close() {
        try {
            this.socket.close();
        } catch (IOException ignored) {}
    }

    @Override
    public void send(String header, byte[] data) {
        try {
            out.writeInt(data.length); // 4-byte length prefix => frame boundary
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to send data", e);
        }
    }

    @Override
    public byte[] receive() {
        try {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            return data;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to receive data", e);
        }
    }
}
