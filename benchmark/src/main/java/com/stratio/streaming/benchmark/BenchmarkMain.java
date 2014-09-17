package com.stratio.streaming.benchmark;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.stratio.streaming.benchmark.exception.BenchmarkException;
import com.stratio.streaming.benchmark.impl.ALotOfQueriesBenchmark;
import com.stratio.streaming.benchmark.impl.BaseSimpleInOutBenchmark;
import com.stratio.streaming.benchmark.impl.OneStreamListenBenchmark;
import com.stratio.streaming.benchmark.impl.SimpleCountQueryBenchmark;
import com.stratio.streaming.benchmark.impl.SimpleMaxQueryBenchmark;
import com.stratio.streaming.benchmark.impl.SimplestQueryBenchmark;

public class BenchmarkMain {

    public static String kafkaHost = "node.stratio.com";
    public static int kafkaPort = 9092;

    public static String zookeeperHost = "node.stratio.com";
    public static int zookeeperPort = 2181;

    public static Integer[] elementsBatchSize = new Integer[] { 10, 1000, 10000
    /*
     * , 100000 , 500000
     */};
    public static Integer iteratees = 1;

    static List<BaseSimpleInOutBenchmark> benchmarks = Arrays.asList(new OneStreamListenBenchmark(kafkaHost, kafkaPort,
            zookeeperHost, zookeeperPort), new SimplestQueryBenchmark(kafkaHost, kafkaPort, zookeeperHost,
            zookeeperPort), new SimpleMaxQueryBenchmark(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort),
            new SimpleCountQueryBenchmark(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort),
            new ALotOfQueriesBenchmark(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort));

    private static Logger log = LoggerFactory.getLogger(BenchmarkMain.class);

    public static void main(String[] args) {
        final MetricRegistry metrics = new MetricRegistry();
        log.info("Init benchmark for {} benchmarkClasses and iterations: {}", benchmarks.size(), elementsBatchSize);
        for (Benchmark benchmark : benchmarks) {
            for (Integer elements : elementsBatchSize) {
                for (int i = 0; i < iteratees; i++) {
                    final String metricName = MetricRegistry
                            .name(benchmark.getClass(), String.valueOf(elements), "req");
                    try {
                        log.info("Processing {} events", elements);
                        final Timer processingTime = metrics.timer(metricName);

                        benchmark.setup(elements);
                        if (i == 0) {
                            benchmark.warmUp();
                        }
                        Timer.Context context = processingTime.time();
                        benchmark.process();
                        context.stop();
                        benchmark.tearDown();
                        log.info("Processed {} events forMeasurement {} in {}", elements, metricName, processingTime
                                .getSnapshot().getMedian());
                    } catch (BenchmarkException e) {
                        log.error("Error processing " + metricName, e);
                    }
                }
            }
        }
        log.info("Benchmark finished.");
        // TODO save data
        System.exit(0);
    }
}
