package com.stratio.streaming.serializer.kafka.impl;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.KryoFactory;
import com.stratio.streaming.serializer.impl.KafkaByteArrayToJavaSerializer;

public class KryoStratioStreamingMessageDecoder implements Decoder<StratioStreamingMessage> {

    private KafkaByteArrayToJavaSerializer kafkaByteArrayToJavaSerializer;

    public KryoStratioStreamingMessageDecoder(VerifiableProperties properties) {
        System.out.println("BOOOOOOOOOOOOOOOOOOooo " + properties);
    }

    @Override
    public StratioStreamingMessage fromBytes(byte[] bytes) {
        return getKafkaByteArrayToJavaSerializer().serialize(bytes);
    }

    private KafkaByteArrayToJavaSerializer getKafkaByteArrayToJavaSerializer() {
        if (kafkaByteArrayToJavaSerializer == null) {
            kafkaByteArrayToJavaSerializer = new KafkaByteArrayToJavaSerializer(KryoFactory.getInstance());
        }
        return kafkaByteArrayToJavaSerializer;
    }
}
