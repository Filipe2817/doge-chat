package com.doge.common;

import java.io.PrintStream;

public abstract class AbstractLogger {
    public void info(String message) {
        this.print(format("INFO", message));
    }
    
    public void warn(String message) {
        this.print(format("WARN", message));
    }

    public void error(String message) {
        this.print(format("ERROR", message));
    }

    public void debug(String message) {
        this.print(format("DEBUG", message));
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

    protected abstract String format(String level, String message);
}
