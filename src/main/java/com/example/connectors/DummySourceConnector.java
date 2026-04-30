package com.example.connectors;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

public class DummySourceConnector extends SourceConnector {

    private Map<String, String> configProps; // Stores connector configuration properties

    @Override
    public String version() {
        return "1.0"; // Returns the version of this connector
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props; // Initialize configuration properties
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DummySourceTask.class; // Specifies the task implementation class
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Provides configuration for each task instance
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps); // Each task gets the same config in this example
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do on connector stop
    }

    @Override
    public ConfigDef config() {
        // Defines the configuration options for this connector
        return new ConfigDef()
                .define("topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Destination topic");
    }
}