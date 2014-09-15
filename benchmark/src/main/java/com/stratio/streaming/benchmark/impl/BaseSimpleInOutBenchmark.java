package com.stratio.streaming.benchmark.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.benchmark.Benchmark;
import com.stratio.streaming.benchmark.exception.BenchmarkException;
import com.stratio.streaming.benchmark.factory.InitTimestampFactory;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.exceptions.StratioAPIGenericException;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.streams.StratioStream;

public abstract class BaseSimpleInOutBenchmark implements Benchmark {

    protected static Logger log = LoggerFactory.getLogger(BaseSimpleInOutBenchmark.class);

    protected final String kafkaHost;

    protected final int kafkaPort;

    protected final String zookeeperHost;

    protected final int zookeeperPort;

    private static Gson gson;
    private Producer<String, String> producer;

    private ConsumerConnector consumer;
    private IStratioStreamingAPI stratioStreamingAPI;

    private List<KeyedMessage<String, String>> data;
    private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

    protected int eventsBatchSize = 0;

    public BaseSimpleInOutBenchmark(String kafkaHost, int kafkaPort, String zookeeperHost, int zookeeperPort) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.zookeeperHost = zookeeperHost;
        this.zookeeperPort = zookeeperPort;
    }

    protected abstract void commandApiActions(IStratioStreamingAPI stratioStreamingAPI)
            throws StratioEngineStatusException, StratioAPISecurityException, StratioEngineOperationException;

    protected abstract String getOutputStreamName();

    protected abstract String getInputStreamName();

    protected abstract int getExpectedOutputEvents();

    protected abstract String getBenchmarkDescription();

    @Override
    public void setup(int eventsBatchSize) throws BenchmarkException {
        log.info("Initializing benchmark with {} elements. Benchmark description: {}", eventsBatchSize,
                getBenchmarkDescription());
        try {
            this.eventsBatchSize = eventsBatchSize;

            stratioStreamingAPI = StratioStreamingAPIFactory.create()
                    .initializeWithServerConfig(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort)
                    .defineAcknowledgeTimeOut(20000);

            boolean createStream = true;
            for (StratioStream stream : stratioStreamingAPI.listStreams()) {
                if (stream.getStreamName().equals(getOutputStreamName())) {
                    createStream = false;
                    break;
                }
            }

            if (createStream) {
                this.commandApiActions(stratioStreamingAPI);
                log.info("Engine initialized.");
            } else {
                log.info("Engine already initialized.");
            }

            gson = new Gson();
            producer = new Producer<String, String>(createProducerConfig());

            consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());

            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(getOutputStreamName(), new Integer(1));
            consumerMap = consumer.createMessageStreams(topicCountMap);

            data = generateStratioStreamingMessages(eventsBatchSize);

            log.info("Finalizing benchmark initialization with {} elements. Benchmark description: {}",
                    eventsBatchSize, getBenchmarkDescription());
        } catch (StratioEngineConnectionException | StratioEngineStatusException | StratioAPIGenericException
                | StratioAPISecurityException | StratioEngineOperationException e) {
            throw new BenchmarkException(e);
        }
    }

    @Override
    public void warmUp() throws BenchmarkException {
        log.info("Warming up benchmark with description: {}", getBenchmarkDescription());
        producer.send(data);
        log.info("Ended warming up for benchmark with description: {}", getBenchmarkDescription());
    }

    @Override
    public void tearDown() throws BenchmarkException {
        log.info("Shutting down benchmark with description: {}", getBenchmarkDescription());
        producer.close();
        consumer.shutdown();
        data = null;
        stratioStreamingAPI = null;
        log.info("Shutted down benchmark with description: {}", getBenchmarkDescription());
    }

    @Override
    public void process() throws BenchmarkException {
        producer.send(data);
        int cutCounter = 0;
        KafkaStream<byte[], byte[]> stream = consumerMap.get(getOutputStreamName()).get(0);

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            this.onEvent(it.next());
            cutCounter++;
            if (cutCounter == getExpectedOutputEvents()) {
                break;
            }
        }
    }

    protected void onEvent(MessageAndMetadata<byte[], byte[]> message) {
        // Nothing to do
    }

    private List<KeyedMessage<String, String>> generateStratioStreamingMessages(int eventsBatchSize) {
        List<KeyedMessage<String, String>> result = new ArrayList<>();

        for (int i = 0; i < eventsBatchSize; i++) {
            StratioStreamingMessage message = new StratioStreamingMessage();

            message.setOperation(STREAM_OPERATIONS.MANIPULATION.INSERT);
            message.setStreamName(getInputStreamName());
            message.setTimestamp(System.currentTimeMillis());
            message.setSession_id(String.valueOf(System.currentTimeMillis()));
            message.setRequest_id(String.valueOf(System.currentTimeMillis()));
            message.setRequest("dummy request");

            List<ColumnNameTypeValue> sensorData = new ArrayList<>();
            sensorData.add(new ColumnNameTypeValue("name", null, "test"));
            sensorData.add(new ColumnNameTypeValue("ind", null, 1));
            sensorData.add(new ColumnNameTypeValue("data", null, i));

            message.setColumns(sensorData);

            result.add(new KeyedMessage<String, String>(InternalTopic.TOPIC_DATA.getTopicName(),
                    STREAM_OPERATIONS.MANIPULATION.INSERT, gson.toJson(message)));
        }
        return result;
    }

    private ProducerConfig createProducerConfig() {
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", kafkaHost + ":" + kafkaPort);
        return new ProducerConfig(properties);
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperHost + ":" + zookeeperPort);
        props.put("group.id", String.valueOf(InitTimestampFactory.getInstance()));
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }
}
