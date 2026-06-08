package com.standard.ibte.bentech;

import com.standard.ibte.bentech.utils.PropertiesUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BentechSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(BentechSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return PropertiesUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        new BentechSinkConfig(props); // validates config at startup
        this.configProps = new HashMap<>(props);
        log.info("BentechSinkConnector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return BentechSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProps));
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("BentechSinkConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return BentechSinkConfig.CONFIG_DEF;
    }
}
