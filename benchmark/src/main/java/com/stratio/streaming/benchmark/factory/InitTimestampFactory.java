package com.stratio.streaming.benchmark.factory;

public class InitTimestampFactory {

    private static Long timestamp;

    private InitTimestampFactory() {
        super();
    }

    public static long getInstance() {
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
