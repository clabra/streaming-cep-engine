package com.stratio.streaming.benchmark.impl;

import java.util.ArrayList;
import java.util.List;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

public class OneStreamListenBenchmark extends BaseSimpleInOutBenchmark {

    private static final String STREAM_NAME = "one_stream_listen_stream";

    public OneStreamListenBenchmark(String kafkaHost, int kafkaPort, String zookeeperHost, int zookeeperPort) {
        super(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort);
    }

    @Override
    protected void commandApiActions(IStratioStreamingAPI stratioStreamingAPI) throws StratioEngineStatusException,
            StratioAPISecurityException, StratioEngineOperationException {
        List<ColumnNameType> columns = new ArrayList<>();
        columns.add(new ColumnNameType("name", ColumnType.STRING));
        columns.add(new ColumnNameType("ind", ColumnType.INTEGER));
        columns.add(new ColumnNameType("data", ColumnType.DOUBLE));

        stratioStreamingAPI.createStream(STREAM_NAME, columns);
        stratioStreamingAPI.listenStream(STREAM_NAME);

    }

    @Override
    protected String getOutputStreamName() {
        return STREAM_NAME;
    }

    @Override
    protected String getInputStreamName() {
        return STREAM_NAME;
    }

    @Override
    protected int getExpectedOutputEvents() {
        return eventsBatchSize;
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Create stream " + STREAM_NAME + " and listen it.";
    }
}
