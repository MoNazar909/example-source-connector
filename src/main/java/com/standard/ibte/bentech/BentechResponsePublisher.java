package com.standard.ibte.bentech;

import com.standard.ibte.bentech.client.BentechApiResponse;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class BentechResponsePublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BentechResponsePublisher.class);

    private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"bentechSinkResponse\",\"namespace\":\"com.standardinsurance.eoi\","
            + "\"fields\":["
            + "{\"name\":\"http_status_code\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"http_status_message\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"response_body\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"error_code\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"error_message\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"retry_count\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"original_topic\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"original_partition\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"original_offset\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"correlation_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"group_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"event_type\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"worker_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"timestamp\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}");

    private static final Schema KEY_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"KafkaKey\",\"namespace\":\"com.standardinsurance.eoi\","
            + "\"fields\":[{\"name\":\"kafka_key\",\"type\":\"string\","
            + "\"doc\":\"Kafka message key: GroupId-WorkerId-TargetBentech-EventType\"}]}");

    private final KafkaProducer<Object, Object> producer;
    private final String responseTopic;
    private final String errorTopic;
    private final String dlqTopic;
    private final BentechSinkRecordMapper mapper;

    public BentechResponsePublisher(BentechSinkConfig config) throws IOException {
        this.responseTopic  = config.getString(BentechSinkConfig.CONNECTOR_RESPONSE_TOPIC);
        this.errorTopic     = config.getString(BentechSinkConfig.CONNECTOR_ERROR_TOPIC);
        this.dlqTopic       = config.getString(BentechSinkConfig.CONNECTOR_DLQ_TOPIC);
        this.mapper         = new BentechSinkRecordMapper();

        String secretsBasePath = config.getString(BentechSinkConfig.CONNECTOR_SECRETS_MOUNT_PATH);
        String kafkaUsername   = readSecret(secretsBasePath + "/ccloud-kafka-credentials/username");
        String kafkaPassword   = readSecret(secretsBasePath + "/ccloud-kafka-credentials/password");
        String srUsername      = readSecret(secretsBasePath + "/ccloud-sr-credentials/username");
        String srPassword      = readSecret(secretsBasePath + "/ccloud-sr-credentials/password");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString(BentechSinkConfig.CONNECTOR_KAFKA_BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        String securityProtocol = config.getString(BentechSinkConfig.CONNECTOR_KAFKA_SECURITY_PROTOCOL);
        props.put("security.protocol", securityProtocol);
        if (securityProtocol.contains("SASL")) {
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + kafkaUsername + "\" password=\"" + kafkaPassword + "\";");
        }

        props.put("schema.registry.url", config.getString(BentechSinkConfig.CONNECTOR_SCHEMA_REGISTRY_URL));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", srUsername + ":" + srPassword);

        this.producer = new KafkaProducer<>(props);
        log.info("BentechResponsePublisher initialised – response: {}, error: {}, dlq: {}",
                responseTopic, errorTopic, dlqTopic);
    }

    public void publishSuccess(SinkRecord record, BentechApiResponse result) {
        Struct value     = (Struct) record.value();
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        writeToTopic(responseTopic, extractKafkaKey(record), buildValue(
                String.valueOf(result.httpCode), httpStatusText(result.httpCode), result.responseBody,
                null, null, null,
                null, null, null,
                mapper.extractCorrelationId(value), mapper.extractGroupId(value),
                mapper.extractEventType(value), mapper.extractWorkerId(value), timestamp));
    }

    public void publishError(SinkRecord record, BentechApiResponse result) {
        Struct value     = (Struct) record.value();
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String errorCode = isSoapFault(result.responseBody) ? "WORKDAY_SOAP_FAULT" : "BENTECH_API_CLIENT_ERROR";
        writeToTopic(errorTopic, extractKafkaKey(record), buildValue(
                String.valueOf(result.httpCode), httpStatusText(result.httpCode), result.responseBody,
                errorCode, truncate(result.responseBody, 1000), null,
                null, null, null,
                mapper.extractCorrelationId(value), mapper.extractGroupId(value),
                mapper.extractEventType(value), mapper.extractWorkerId(value), timestamp));
    }

    public void publishDlq(SinkRecord record, BentechApiResponse result, int retryCount) {
        Struct value     = (Struct) record.value();
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String dlqMessage = result.errorMessage + " – after " + retryCount + " retries. Record written to DLQ for manual remediation.";
        writeToTopic(dlqTopic, extractKafkaKey(record), buildValue(
                result.httpCode > 0 ? String.valueOf(result.httpCode) : null,
                result.httpCode > 0 ? httpStatusText(result.httpCode) : null,
                result.responseBody,
                "HTTP_SINK_MAX_RETRIES_EXCEEDED", dlqMessage,
                String.valueOf(retryCount),
                record.topic(), String.valueOf(record.kafkaPartition()), String.valueOf(record.kafkaOffset()),
                mapper.extractCorrelationId(value), mapper.extractGroupId(value),
                mapper.extractEventType(value), mapper.extractWorkerId(value), timestamp));
    }

    public void publishInternalError(SinkRecord record, String errorMessage) {
        Struct value     = (Struct) record.value();
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        writeToTopic(dlqTopic, extractKafkaKey(record), buildValue(
                null, null, null,
                "CONNECTOR_INTERNAL_ERROR", errorMessage, null,
                record.topic(), String.valueOf(record.kafkaPartition()), String.valueOf(record.kafkaOffset()),
                mapper.extractCorrelationId(value), mapper.extractGroupId(value),
                mapper.extractEventType(value), mapper.extractWorkerId(value), timestamp));
    }

    private void writeToTopic(String topic, String kafkaKeyStr, GenericRecord valueRecord) {
        try {
            GenericRecord key = new GenericData.Record(KEY_SCHEMA);
            key.put("kafka_key", kafkaKeyStr);
            producer.send(new ProducerRecord<>(topic, key, valueRecord), (meta, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish to topic '{}': {}", topic, ex.getMessage());
                } else {
                    log.debug("Published – topic: {}, partition: {}, offset: {}",
                            meta.topic(), meta.partition(), meta.offset());
                }
            });
        } catch (Exception e) {
            log.error("Failed to write to topic {}: {}", topic, e.getMessage());
        }
    }

    private GenericRecord buildValue(
            String httpStatusCode, String httpStatusMessage, String responseBody,
            String errorCode, String errorMessage, String retryCount,
            String originalTopic, String originalPartition, String originalOffset,
            String correlationId, String groupId, String eventType, String workerId, String timestamp) {

        GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
        record.put("http_status_code",    httpStatusCode);
        record.put("http_status_message", httpStatusMessage);
        record.put("response_body",       responseBody);
        record.put("error_code",          errorCode);
        record.put("error_message",       errorMessage);
        record.put("retry_count",         retryCount);
        record.put("original_topic",      originalTopic);
        record.put("original_partition",  originalPartition);
        record.put("original_offset",     originalOffset);
        record.put("correlation_id",      correlationId);
        record.put("group_id",            groupId);
        record.put("event_type",          eventType);
        record.put("worker_id",           workerId);
        record.put("timestamp",           timestamp);
        return record;
    }

    private String extractKafkaKey(SinkRecord record) {
        if (record.key() instanceof Struct) {
            Object v = ((Struct) record.key()).get("kafka_key");
            return v != null ? v.toString() : "unknown";
        }
        return record.key() != null ? record.key().toString() : "unknown";
    }

    private boolean isSoapFault(String body) {
        return body != null && (body.contains("<Fault>") || body.contains(":Fault>"));
    }

    private String httpStatusText(int code) {
        switch (code) {
            case 200: return "OK";
            case 201: return "Created";
            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 429: return "Too Many Requests";
            case 500: return "Internal Server Error";
            case 502: return "Bad Gateway";
            case 503: return "Service Unavailable";
            case 504: return "Gateway Timeout";
            default:  return "HTTP " + code;
        }
    }

    private String truncate(String s, int maxLength) {
        if (s == null) return null;
        return s.length() > maxLength ? s.substring(0, maxLength) + "..." : s;
    }

    private String readSecret(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path))).trim();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        log.info("BentechResponsePublisher closed");
    }
}
