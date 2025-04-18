package com.doge.chat.server;

import com.doge.common.AbstractLogger;

public class Logger extends AbstractLogger {
    @Override
    protected void print(String message) {
        System.out.println(message);
    }
}
