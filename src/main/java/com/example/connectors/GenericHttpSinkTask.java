package com.example.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class GenericHttpSinkTask extends SinkTask {

    private static final Schema VALUE_SCHEMA = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"HcmSinkResponse\",\"namespace\":\"com.standardinsurance.eoi\","
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
        + "\"doc\":\"Kafka message key: GroupId-WorkerId-Target_HCM-EventType\"}]}");

    private HttpClient httpClient;
    private ObjectMapper objectMapper;
    private Map<String, TokenInfo> tokenCache;
    private KafkaProducer<Object, Object> producer;

    private String secretsBasePath;
    private String contentType;
    private String responseTopic;
    private String errorTopic;
    private String dlqTopic;
    private int maxRetries;
    private long retryBackoffMs;

    private static class TokenInfo {
        final String accessToken;
        final Instant expiresAt;

        TokenInfo(String accessToken, int expiresInSeconds) {
            this.accessToken = accessToken;
            this.expiresAt = Instant.now().plusSeconds(expiresInSeconds - 60);
        }

        boolean isExpired() {
            return Instant.now().isAfter(expiresAt);
        }
    }

    private static class ApiResponse {
        final int statusCode;
        final String body;
        final int retryCount;

        ApiResponse(int statusCode, String body, int retryCount) {
            this.statusCode = statusCode;
            this.body = body;
            this.retryCount = retryCount;
        }
    }

    @Override
    public String version() { return "1.0"; }

    @Override
    public void start(Map<String, String> props) {
        try {
            this.httpClient = HttpClient.newHttpClient();
            this.objectMapper = new ObjectMapper();
            this.tokenCache = new ConcurrentHashMap<>();
            this.secretsBasePath = props.getOrDefault("secrets.base.path", "/etc/secrets");
            this.contentType = props.get("content.type");
            this.responseTopic = props.get("response.topic");
            this.errorTopic = props.get("error.topic");
            this.dlqTopic = props.get("dlq.topic");
            this.maxRetries = Integer.parseInt(props.getOrDefault("max.retries", "3"));
            this.retryBackoffMs = Long.parseLong(props.getOrDefault("retry.backoff.ms", "1000"));

            String kafkaUsername = readSecret(secretsBasePath + "/ccloud-kafka-credentials/username");
            String kafkaPassword = readSecret(secretsBasePath + "/ccloud-kafka-credentials/password");
            String srUsername    = readSecret(secretsBasePath + "/ccloud-sr-credentials/username");
            String srPassword    = readSecret(secretsBasePath + "/ccloud-sr-credentials/password");

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get("kafka.bootstrap.servers"));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            producerProps.put("security.protocol", "SASL_SSL");
            producerProps.put("sasl.mechanism", "PLAIN");
            producerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='"
                + kafkaUsername + "' password='" + kafkaPassword + "';");
            producerProps.put("schema.registry.url", props.get("schema.registry.url"));
            producerProps.put("basic.auth.credentials.source", "USER_INFO");
            producerProps.put("basic.auth.user.info", srUsername + ":" + srPassword);

            this.producer = new KafkaProducer<>(producerProps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read credentials from secrets: " + e.getMessage(), e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            processRecord(record);
        }
    }

    private void processRecord(SinkRecord record) {
        String groupId = null;
        String correlationId = null;
        String eventType = null;
        String workerId = null;

        try {
            Struct value = (Struct) record.value();
            groupId     = (String) value.get("group_id");
            correlationId = (String) value.get("correlation_id");
            eventType   = (String) value.get("event_type");

            JsonNode flattenedEvent = objectMapper.readTree((String) value.get("flattened_event"));
            workerId  = flattenedEvent.has("worker_id") ? flattenedEvent.get("worker_id").asText(null) : null;
            String tokenUrl = flattenedEvent.get("target_api_token_url").asText();
            String apiUrl   = flattenedEvent.get("target_api_url").asText();
            String payload  = (String) value.get("target_HCM_payload");

            System.out.printf("[%s][partition=%d][offset=%d] key=%s groupId=%s apiUrl=%s%n",
                record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                record.key(), groupId, apiUrl);

            String token = getOrRefreshToken(groupId, tokenUrl);
            ApiResponse response = callApiWithRetry(apiUrl, token, payload);

            routeResponse(record, response, groupId, correlationId, eventType, workerId);

        } catch (Exception e) {
            System.err.printf("Internal error at offset %d: %s%n", record.kafkaOffset(), e.getMessage());
            writeToDlq(record, groupId, correlationId, eventType, workerId, toStackTrace(e), "CONNECTOR_INTERNAL_ERROR", 0);
        }
    }

    private void routeResponse(SinkRecord record, ApiResponse response,
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

    private void writeToDlq(SinkRecord record, String groupId, String correlationId, String eventType, String workerId,
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

    private void writeToTopic(String topic, String kafkaKeyStr, GenericRecord value) {
        try {
            GenericRecord key = new GenericData.Record(KEY_SCHEMA);
            key.put("kafka_key", kafkaKeyStr);
            producer.send(new ProducerRecord<>(topic, key, value));
            System.out.println("Written to topic: " + topic);
        } catch (Exception e) {
            System.err.println("Failed to write to topic " + topic + ": " + e.getMessage());
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

    private ApiResponse callApiWithRetry(String apiUrl, String token, String payload) throws Exception {
        int attempts = 0;
        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

            HttpResponse<String> httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = httpResponse.statusCode();
            String body    = httpResponse.body();

            boolean retryable = (statusCode == 429 || statusCode >= 500) && !isSoapFault(body);
            if (!retryable || attempts >= maxRetries) {
                return new ApiResponse(statusCode, body, attempts);
            }

            attempts++;
            System.out.printf("Transient HTTP %d, retry %d/%d%n", statusCode, attempts, maxRetries);
            Thread.sleep(retryBackoffMs);
        }
    }

    private String getOrRefreshToken(String groupId, String tokenUrl) throws Exception {
        TokenInfo cached = tokenCache.get(groupId);
        if (cached != null && !cached.isExpired()) {
            return cached.accessToken;
        }

        String secretPath   = secretsBasePath + "/hcm-connector-tenant-secrets/";
        String clientId     = readSecret(secretPath + groupId + "_clientId");
        String clientSecret = readSecret(secretPath + groupId + "_clientsecret");
        String refreshToken = readSecret(secretPath + groupId + "_refreshtoken");

        String form = "grant_type=refresh_token"
            + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8)
            + "&client_id="     + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
            + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Token request failed: HTTP " + response.statusCode() + " " + response.body());
        }

        JsonNode json       = objectMapper.readTree(response.body());
        String accessToken  = json.get("access_token").asText();
        int expiresIn       = json.has("expires_in") ? json.get("expires_in").asInt() : 3600;

        tokenCache.put(groupId, new TokenInfo(accessToken, expiresIn));
        System.out.println("Token refreshed for group: " + groupId);
        return accessToken;
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

    private String toStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private String readSecret(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path))).trim();
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }
}
