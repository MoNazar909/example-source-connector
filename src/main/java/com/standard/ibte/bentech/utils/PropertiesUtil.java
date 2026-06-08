package com.standard.ibte.bentech.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public final class PropertiesUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

    private static final String CONNECTOR_VERSION  = "connector.version";
    private static final String DEFAULT_VERSION    = "1.0.0";
    private static final String PROPERTIES_FILE    = "bentech-sink-connector-app.properties";

    private static final Properties properties;

    static {
        properties = new Properties();
        try (InputStream stream = PropertiesUtil.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (stream != null) {
                properties.load(stream);
            } else {
                log.warn("Properties file not found on classpath: {}", PROPERTIES_FILE);
            }
        } catch (Exception ex) {
            log.warn("Error while loading properties: ", ex);
        }

        String version = properties.getProperty(CONNECTOR_VERSION);
        if (version == null || version.isBlank()) {
            version = DEFAULT_VERSION;
            properties.setProperty(CONNECTOR_VERSION, version);
        }
        log.info("Connector plugin version: {}", version);
    }

    private PropertiesUtil() {}

    public static String getVersion() {
        return properties.getProperty(CONNECTOR_VERSION, DEFAULT_VERSION);
    }
}
