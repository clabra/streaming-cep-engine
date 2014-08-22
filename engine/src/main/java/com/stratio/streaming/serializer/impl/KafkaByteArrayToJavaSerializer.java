package com.stratio.streaming.serializer.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.serializer.ListElementSerializerHandler;

public class KafkaByteArrayToJavaSerializer extends ListElementSerializerHandler<byte[], StratioStreamingMessage> {

    private static final long serialVersionUID = 7193429429302067377L;

    protected static Logger log = LoggerFactory.getLogger(KafkaByteArrayToJavaSerializer.class);

    private final Kryo kryo;

    public KafkaByteArrayToJavaSerializer(Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    public StratioStreamingMessage serialize(byte[] object) {
        StratioStreamingMessage result = null;
        try (Input input = new Input(object);) {
            result = kryo.readObject(input, StratioStreamingMessage.class);
        }
        return result;
    }

    @Override
    public byte[] deserialize(StratioStreamingMessage object) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream, 4096)) {
            kryo.writeObject(output, object);
            output.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Error serializing streaming message", e);
            return null;
        }
    }

}
