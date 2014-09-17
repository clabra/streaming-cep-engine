package com.stratio.streaming.benchmark.impl;

import java.util.ArrayList;
import java.util.List;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

public class ALotOfQueriesBenchmark extends BaseSimpleInOutBenchmark {

    private static final String IN_STREAM = "thousand_queries_listen_in_stream";
    private static final String OUT_STREAM = "thousand_queries_listen_out_stream";

    private static final int QUERIES = 100;

    public ALotOfQueriesBenchmark(String kafkaHost, int kafkaPort, String zookeeperHost, int zookeeperPort) {
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

        for (int i = 0; i < QUERIES; i++) {
            stratioStreamingAPI.addQuery(IN_STREAM, "from " + IN_STREAM + " select name,ind,data,'" + i
                    + "' as count insert into " + OUT_STREAM);
            log.info("Added query number {}", i);
        }

        stratioStreamingAPI.listenStream(OUT_STREAM);

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
        return eventsBatchSize * QUERIES;
    }

    @Override
    protected String getBenchmarkDescription() {
        return "Create stream a thousand queries.";
    }
}
