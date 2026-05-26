package com.example.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class GenericHttpSinkTask extends SinkTask {

    private WorkdayApiClient apiClient;
    private KafkaEventRouter router;
    private ObjectMapper objectMapper;

    @Override
    public String version() { return "1.0"; }

    @Override
    public void start(Map<String, String> props) {
        try {
            String secretsBasePath = props.getOrDefault("secrets.base.path", "/etc/secrets");
            String contentType     = props.get("content.type");
            int maxRetries         = Integer.parseInt(props.getOrDefault("max.retries", "3"));
            long retryBackoffMs    = Long.parseLong(props.getOrDefault("retry.backoff.ms", "1000"));

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

            KafkaProducer<Object, Object> producer = new KafkaProducer<>(producerProps);

            this.objectMapper = new ObjectMapper();
            this.apiClient = new WorkdayApiClient(secretsBasePath, contentType, maxRetries, retryBackoffMs);
            this.router = new KafkaEventRouter(producer,
                props.get("response.topic"),
                props.get("error.topic"),
                props.get("dlq.topic"));

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
        String groupId       = null;
        String correlationId = null;
        String eventType     = null;
        String workerId      = null;

        try {
            Struct value  = (Struct) record.value();
            groupId       = (String) value.get("group_id");
            correlationId = (String) value.get("correlation_id");
            eventType     = (String) value.get("event_type");

            JsonNode flattenedEvent = objectMapper.readTree((String) value.get("flattened_event"));
            workerId       = flattenedEvent.has("worker_id") ? flattenedEvent.get("worker_id").asText(null) : null;
            String tokenUrl = flattenedEvent.get("target_api_token_url").asText();
            String apiUrl   = flattenedEvent.get("target_api_url").asText();
            String payload  = (String) value.get("target_HCM_payload");

            System.out.printf("[%s][partition=%d][offset=%d] key=%s groupId=%s apiUrl=%s%n",
                record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                record.key(), groupId, apiUrl);

            String token          = apiClient.getOrRefreshToken(groupId, tokenUrl);
            ApiResponse response  = apiClient.callApiWithRetry(apiUrl, token, payload);

            router.routeResponse(record, response, groupId, correlationId, eventType, workerId);

        } catch (Exception e) {
            System.err.printf("Internal error at offset %d: %s%n", record.kafkaOffset(), e.getMessage());
            router.writeToDlq(record, groupId, correlationId, eventType, workerId,
                toStackTrace(e), "CONNECTOR_INTERNAL_ERROR", 0);
        }
    }

    @Override
    public void stop() {
        if (router != null) {
            router.close();
        }
    }

    private String toStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private String readSecret(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path))).trim();
    }
}
