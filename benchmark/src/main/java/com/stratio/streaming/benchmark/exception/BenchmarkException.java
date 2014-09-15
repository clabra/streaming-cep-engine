package com.stratio.streaming.benchmark.exception;

public class BenchmarkException extends Exception {

    private static final long serialVersionUID = -8436194015916507644L;

    public BenchmarkException() {
        super();
    }

    public BenchmarkException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public BenchmarkException(String message, Throwable cause) {
        super(message, cause);
    }

    public BenchmarkException(String message) {
        super(message);
    }

    public BenchmarkException(Throwable cause) {
        super(cause);
    }

}
