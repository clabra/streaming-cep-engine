package com.stratio.streaming.benchmark;

import org.apache.commons.cli.Option;

public enum BenchmarkOption {
    KAFKA_CLUSTER("kafkaCluster", "Kafka cluster separated by commas", false),
    KAFKA_PORT("kafkaPort", "Kafka port", false),
    ZK_CLUSTER("kafkaCluster", "Zookeeper cluster separated by commas", false),
    ZK_PORT("kafkaPort", "Zookeeper port", false);

    private final Option option;

    private BenchmarkOption(String key, String description, boolean optional) {
        Option option = new Option(key, true, description);
        option.setOptionalArg(optional);
        this.option = option;
    }

    public Option getOption() {
        return option;
    }
}