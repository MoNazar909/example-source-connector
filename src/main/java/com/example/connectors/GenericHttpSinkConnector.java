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
                .define("topics",                ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Source topic")
                .define("content.type",          ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Content-Type header for outbound API calls")
                .define("secrets.base.path",     ConfigDef.Type.STRING, "/etc/secrets", ConfigDef.Importance.HIGH, "Base path for mounted secrets volume")
                .define("response.topic",        ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Topic for successful API responses")
                .define("error.topic",           ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Topic for Workday business errors")
                .define("dlq.topic",             ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Topic for failed messages after retries or internal errors")
                .define("max.retries",           ConfigDef.Type.INT,    3,              ConfigDef.Importance.MEDIUM, "Max retry attempts for transient failures")
                .define("retry.backoff.ms",      ConfigDef.Type.LONG,   1000L,          ConfigDef.Importance.MEDIUM, "Delay between retries in milliseconds")
                .define("kafka.bootstrap.servers", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka bootstrap servers for the internal producer")
                .define("schema.registry.url",   ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,   "Schema Registry URL for Avro serialization");
    }
}