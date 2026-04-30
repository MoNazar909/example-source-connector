package com.example.connectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;

public class DummySourceTask extends SourceTask {

    private String topic; // Kafka topic to which records will be sent
    private int counter = 0; // Counter to generate unique message values

    @Override
    public String version() {
        return "1.0"; // Returns the version of this connector
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get("topic"); // Initialize topic from connector configuration
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000); // Simulate delay between records

        // Define source partition and offset for Kafka Connect
        Map<String, String> sourcePartition = Collections.singletonMap("source", "dummy");
        Map<String, Long> sourceOffset = Collections.singletonMap("position", (long) counter);

        // Generate a dummy message value
        String value = "dummy-message-" + counter++;

        // Create a SourceRecord to send to Kafka
        SourceRecord record = new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                Schema.STRING_SCHEMA,
                value
        );

        // Return the record as a singleton list
        return Collections.singletonList(record);
    }

    @Override
    public void stop() {
        // Cleanup resources if needed when the task stops
    }
}