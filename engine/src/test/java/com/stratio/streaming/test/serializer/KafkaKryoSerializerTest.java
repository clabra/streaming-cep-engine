package com.stratio.streaming.test.serializer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.KryoFactory;
import com.stratio.streaming.serializer.impl.KafkaByteArrayToJavaSerializer;

public class KafkaKryoSerializerTest {

    private KafkaByteArrayToJavaSerializer serializer;

    private static final byte[] BYTE_ARRAY_GOOD_OBJECT = new byte[] { 1, 0, 0, 1, 84, 69, 83, 84, 95, 79, 80, 69, 82,
            65, 84, 73, 79, -50, 0, 1, 84, 69, 83, 84, 95, 82, 69, -47, 1, 84, 69, 83, 84, 95, 82, 69, 81, 95, 73, -60,
            1, 84, 69, 83, 84, 95, 83, 69, 83, 83, 73, 79, 78, 95, 73, -60, 1, 84, 69, 83, 84, 95, 83, 84, 82, 69, 65,
            77, 95, 78, 65, 77, -59, 1, -66, -62, -17, -121, -1, 81, 0 };

    @Before
    public void setUp() {
        serializer = new KafkaByteArrayToJavaSerializer(KryoFactory.getInstance());
    }

    @Test
    public void simpleObjectToByteArrayTest() {
        byte[] result = serializer.deserialize(getSimpleObject());
        Assert.assertNotNull(result);
        Assert.assertEquals(81, result.length);
    }

    @Test
    public void simplByteArrayToObjectTest() {
        StratioStreamingMessage result = serializer.serialize(BYTE_ARRAY_GOOD_OBJECT);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof StratioStreamingMessage);
        Assert.assertEquals("TEST_STREAM_NAME", result.getStreamName());
    }

    private StratioStreamingMessage getSimpleObject() {
        return new StratioStreamingMessage("TEST_OPERATION", "TEST_STREAM_NAME", "TEST_SESSION_ID", "TEST_REQ_ID",
                "TEST_REQ", System.currentTimeMillis(), null, null, null);
    }
}
