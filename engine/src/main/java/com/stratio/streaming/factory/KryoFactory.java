package com.stratio.streaming.factory;

import com.esotericsoftware.kryo.Kryo;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class KryoFactory {

    private static Kryo INSTANCE;

    public static Kryo getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Kryo();
            INSTANCE.register(StratioStreamingMessage.class);
        }
        return INSTANCE;
    }
}
