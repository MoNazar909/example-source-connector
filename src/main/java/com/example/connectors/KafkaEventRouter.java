package com.example.connectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

class KafkaEventRouter {

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
        + "\"doc\":\"Kafka message key: GroupId-WorkerId-Target_bentech-EventType\"}]}");

    private final KafkaProducer<Object, Object> producer;
    private final String responseTopic;
    private final String errorTopic;
    private final String dlqTopic;

    KafkaEventRouter(KafkaProducer<Object, Object> producer,
                     String responseTopic, String errorTopic, String dlqTopic) {
        this.producer = producer;
        this.responseTopic = responseTopic;
        this.errorTopic = errorTopic;
        this.dlqTopic = dlqTopic;
    }

    void routeResponse(SinkRecord record, ApiResponse response,
                       String groupId, String correlationId, String eventType, String workerId) {
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String kafkaKey  = extractKafkaKey(record);
        int status = response.statusCode;

        if (status >= 200 && status < 300) {
            writeToTopic(responseTopic, kafkaKey, buildValue(
                String.valueOf(status), httpStatusMessage(status), response.body,
                null, null, null,
                null, null, null,
                correlationId, groupId, eventType, workerId, timestamp));

        } else if ((status >= 400 && status < 500 && status != 429) || isSoapFault(response.body)) {
            String errorCode = isSoapFault(response.body) ? "WORKDAY_SOAP_FAULT" : "WORKDAY_API_CLIENT_ERROR";
            writeToTopic(errorTopic, kafkaKey, buildValue(
                String.valueOf(status), httpStatusMessage(status), response.body,
                errorCode, truncate(response.body, 1000),
                null, null, null, null,
                correlationId, groupId, eventType, workerId, timestamp));

        } else {
            writeToDlq(record, groupId, correlationId, eventType, workerId,
                "HTTP " + status + " after " + response.retryCount + " retries: " + truncate(response.body, 1000),
                "HTTP_SINK_MAX_RETRIES_EXCEEDED", response.retryCount);
        }
    }

    void writeToDlq(SinkRecord record, String groupId, String correlationId, String eventType, String workerId,
                    String errorMessage, String errorCode, int retryCount) {
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String kafkaKey  = extractKafkaKey(record);
        writeToTopic(dlqTopic, kafkaKey, buildValue(
            null, null, null,
            errorCode, errorMessage,
            retryCount > 0 ? String.valueOf(retryCount) : null,
            record.topic(), String.valueOf(record.kafkaPartition()), String.valueOf(record.kafkaOffset()),
            correlationId, groupId, eventType, workerId, timestamp));
    }

    void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private void writeToTopic(String topic, String kafkaKeyStr, GenericRecord value) {
        try {
            GenericRecord key = new GenericData.Record(KEY_SCHEMA);
            key.put("kafka_key", kafkaKeyStr);
            producer.send(new ProducerRecord<>(topic, key, value));
            System.out.println("Written to topic: " + topic);
        } catch (Exception e) {
            System.err.println("Failed to write to topic " + topic + ": " + e.getMessage());
            e.printStackTrace(System.err);
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

    private boolean isSoapFault(String body) {
        return body != null && (body.contains("<Fault>") || body.contains(":Fault>"));
    }

    private String extractKafkaKey(SinkRecord record) {
        if (record.key() instanceof Struct) {
            Object v = ((Struct) record.key()).get("kafka_key");
            return v != null ? v.toString() : "unknown";
        }
        return record.key() != null ? record.key().toString() : "unknown";
    }

    private String httpStatusMessage(int code) {
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
}
