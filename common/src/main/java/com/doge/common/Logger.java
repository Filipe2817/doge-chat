package com.doge.common;

import java.lang.StackWalker.StackFrame;

public class Logger extends AbstractLogger {
    private static final StackWalker WALKER = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

    @Override
    protected void print(String message) {
        System.out.println(message);
    }
    
    @Override
    protected String format(String level, String message) {
        String caller = WALKER.walk(frames ->
            frames
                .map(StackFrame::getDeclaringClass)
                .filter(c -> !AbstractLogger.class.isAssignableFrom(c))
                .filter(c -> c != Thread.class)
                .findFirst()
                .map(Class::getSimpleName)
                .orElse("Unknown")
        );

        return String.format("[%s] [%s] %s", level, caller, message);
    }
}
