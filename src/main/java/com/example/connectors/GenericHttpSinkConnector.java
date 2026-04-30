package com.example.connectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.*;

public class GenericHttpSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public String version() { return "1.0"; }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GenericHttpSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("topics", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source topic");
    }
}