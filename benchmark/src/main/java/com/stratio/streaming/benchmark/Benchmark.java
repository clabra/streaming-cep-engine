package com.stratio.streaming.benchmark;

import com.stratio.streaming.benchmark.exception.BenchmarkException;

public interface Benchmark {

    void setup(int eventsBatchSize) throws BenchmarkException;

    void warmUp() throws BenchmarkException;

    void tearDown() throws BenchmarkException;

    public void process() throws BenchmarkException;
}
