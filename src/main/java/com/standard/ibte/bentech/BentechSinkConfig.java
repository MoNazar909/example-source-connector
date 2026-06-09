package com.standard.ibte.bentech;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class BentechSinkConfig extends AbstractConfig {

    public static final String CONNECTOR_SECRETS_MOUNT_PATH          = "secrets.base.path";
    public static final String CONNECTOR_REQUEST_CONTENT_TYPE        = "content.type";
    public static final String CONNECTOR_RESPONSE_TOPIC              = "response.topic";
    public static final String CONNECTOR_ERROR_TOPIC                 = "error.topic";
    public static final String CONNECTOR_DLQ_TOPIC                   = "dlq.topic";
    public static final String CONNECTOR_RETRY_MAX                   = "max.retries";
    public static final String CONNECTOR_RETRY_BACKOFF_MS            = "retry.backoff.ms";
    public static final String CONNECTOR_KAFKA_BOOTSTRAP_SERVERS     = "kafka.bootstrap.servers";
    public static final String CONNECTOR_SCHEMA_REGISTRY_URL         = "schema.registry.url";
    public static final String CONNECTOR_CONNECTOR_NAME              = "connector.name";
    public static final String CONNECTOR_KAFKA_SECURITY_PROTOCOL     = "connector.kafka.security.protocol";
    public static final String CONNECTOR_TOKEN_REFRESH_BUFFER_SECONDS = "connector.token.refresh.buffer.seconds";
    public static final String CONNECTOR_TOKEN_CACHE_MINUTES          = "connector.token.cache.minutes";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTOR_SECRETS_MOUNT_PATH,
                    ConfigDef.Type.STRING, "/etc/secrets",
                    ConfigDef.Importance.HIGH,
                    "Base path for mounted secrets volume.")
            .define(CONNECTOR_REQUEST_CONTENT_TYPE,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Content-Type header for outbound API calls (e.g. application/xml).")
            .define(CONNECTOR_RESPONSE_TOPIC,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Kafka topic for successful API responses (HTTP 2xx).")
            .define(CONNECTOR_ERROR_TOPIC,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Kafka topic for data errors – SOAP faults and HTTP 4xx responses (no retry).")
            .define(CONNECTOR_DLQ_TOPIC,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Kafka topic for records that failed after all retries are exhausted.")
            .define(CONNECTOR_RETRY_MAX,
                    ConfigDef.Type.INT, 3,
                    ConfigDef.Importance.MEDIUM,
                    "Maximum retry attempts for transient system errors (HTTP 5xx without SOAP fault).")
            .define(CONNECTOR_RETRY_BACKOFF_MS,
                    ConfigDef.Type.LONG, 1000L,
                    ConfigDef.Importance.MEDIUM,
                    "Delay between retries in milliseconds.")
            .define(CONNECTOR_KAFKA_BOOTSTRAP_SERVERS,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Bootstrap servers for the internal KafkaProducer used to publish response/error/DLQ messages.")
            .define(CONNECTOR_SCHEMA_REGISTRY_URL,
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Schema Registry URL for the internal KafkaProducer.")
            .define(CONNECTOR_CONNECTOR_NAME,
                    ConfigDef.Type.STRING, "bentech-sink-connector",
                    ConfigDef.Importance.MEDIUM,
                    "Logical name of this connector deployment. Used in DLQ error headers.")
            .define(CONNECTOR_KAFKA_SECURITY_PROTOCOL,
                    ConfigDef.Type.STRING, "SASL_SSL",
                    ConfigDef.ValidString.in("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"),
                    ConfigDef.Importance.MEDIUM,
                    "Security protocol for the internal KafkaProducer. Use SASL_SSL for Confluent Cloud, PLAINTEXT for local dev.")
            .define(CONNECTOR_TOKEN_REFRESH_BUFFER_SECONDS,
                    ConfigDef.Type.INT, 0,
                    ConfigDef.Range.between(0, 3600),
                    ConfigDef.Importance.LOW,
                    "Seconds before token expiry to proactively refresh. 0 disables proactive refresh (Workday default).")
            .define(CONNECTOR_TOKEN_CACHE_MINUTES,
                    ConfigDef.Type.INT, 60,
                    ConfigDef.Range.between(1, 1440),
                    ConfigDef.Importance.MEDIUM,
                    "Maximum number of minutes to cache an access token. Actual expiry is min(expires_in, this value) minus the refresh buffer.");

    public BentechSinkConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals);
    }
}
