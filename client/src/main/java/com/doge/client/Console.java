package com.doge.client;

import java.io.IOException;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import com.doge.common.AbstractLogger;

public class Console extends AbstractLogger {
    private final LineReader reader;
    private final String prompt = "client> ";

    public Console() throws IOException {
        Terminal terminal = TerminalBuilder.builder()
                .jansi(true)
                .dumb(true)
                .build();

        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();

        this.reader = reader;
    }
    
    public String readLine(String prompt) {
        return reader.readLine(prompt);
    }

    public String readLine() {
        return this.readLine(this.prompt);
    }

    public void close() throws IOException {
        reader.getTerminal().close();
    }

    @Override
    protected void print(String message) {
        reader.printAbove(message);
    }

    @Override
    protected String format(String level, String message) {
        return String.format("[%s] %s", level, message);
    }
}
