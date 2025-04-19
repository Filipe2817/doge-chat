package com.doge.common;

import java.io.PrintStream;

public abstract class AbstractLogger {
    private final String formatString = "[%s] %s";

    public void info(String message) {
        print(format("INFO", message));
    }
    
    public void warn(String message) {
        print(format("WARN", message));
    }

    public void error(String message) {
        print(format("ERROR", message));
    }

    public void debug(String message) {
        print(format("DEBUG", message));
    }

    private String format(String level, String message) {
        return String.format(this.formatString, level, message);
    }

    public void alterSystemPrint() {
        System.setOut(new PrintStream(System.out) {
            @Override
            public void println(String x) {
                info(x);
            }
        });

        System.setErr(new PrintStream(System.err) {
            @Override
            public void println(String x) {
                error(x);
            }
        });
    }

    protected abstract void print(String message);
}
