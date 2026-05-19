package com.example.connectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class GenericHttpSinkTask extends SinkTask {

    @Override
    public String version() { return "1.0"; }

    @Override
    public void start(Map<String, String> props) {}

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            System.out.printf("[%s][partition=%d][offset=%d] %s%n",
                record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.value());
        }
    }

    @Override
    public void stop() {}
}