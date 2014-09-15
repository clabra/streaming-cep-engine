package com.stratio.streaming.benchmark.impl;

import java.util.ArrayList;
import java.util.List;

import kafka.message.MessageAndMetadata;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

public class SimpleCountQueryBenchmark extends BaseSimpleInOutBenchmark {

    private static final String OUT_STREAM = "s_c_q_out_stream";
    private static final String IN_STREAM = "s_c_q_in_stream";

    public SimpleCountQueryBenchmark(String kafkaHost, int kafkaPort, String zookeeperHost, int zookeeperPort) {
        super(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort);
    }

    @Override
    protected void commandApiActions(IStratioStreamingAPI stratioStreamingAPI) throws StratioEngineStatusException,
            StratioAPISecurityException, StratioEngineOperationException {
        List<ColumnNameType> columns = new ArrayList<>();
        columns.add(new ColumnNameType("name", ColumnType.STRING));
        columns.add(new ColumnNameType("ind", ColumnType.INTEGER));
        columns.add(new ColumnNameType("data", ColumnType.DOUBLE));

        stratioStreamingAPI.createStream(IN_STREAM, columns);
        stratioStreamingAPI.addQuery(IN_STREAM, "from " + IN_STREAM
                + " select count(*) as count group by ind insert into " + OUT_STREAM);
        stratioStreamingAPI.listenStream(OUT_STREAM);

    }

    @Override
    protected void onEvent(MessageAndMetadata<byte[], byte[]> message) {
        log.info("Message output data: {}", new String(message.message()));
    }

    @Override
    protected String getOutputStreamName() {
        return OUT_STREAM;
    }

    @Override
    protected String getInputStreamName() {
        return IN_STREAM;
    }

    @Override
    protected int getExpectedOutputEvents() {
        return 1;
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Simple count siddhi query.";
    }

}
