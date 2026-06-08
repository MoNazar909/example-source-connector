package com.standard.ibte.bentech;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Extracts fields from a SinkRecord whose value is an Avro Struct.
 * All URL and metadata fields are nested inside the flattenedEvent JSON string.
 */
public class BentechSinkRecordMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Extracts the target_bentech_payload (SOAP XML) from the record value.
     * Removes JSON escape sequences so the result is valid XML.
     */
    public String extractSoapPayload(SinkRecord record) {
        Struct value = (Struct) record.value();
        String raw = (String) value.get("targetBentechPayload");
        if (raw == null) {
            throw new IllegalArgumentException("targetBentechPayload is null or missing in record value");
        }
        return raw
                .replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\/", "/");
    }

    public String extractApiUrl(Struct value) {
        return extractFromEventMetaData(value, "targetHostname");
    }

    public String extractTokenUrl(Struct value) {
        return extractFromEventMetaData(value, "targetTokenHostname");
    }

    public String extractWorkerId(Struct value) {
        return extractFromEventMetaData(value, "workerId");
    }

    public String extractGroupId(Struct value) {
        return (String) value.get("groupId");
    }

    public String extractCorrelationId(Struct value) {
        return (String) value.get("correlationId");
    }

    public String extractEventType(Struct value) {
        return (String) value.get("eventType");
    }

    private String extractFromEventMetaData(Struct value, String fieldName) {
        try {
            String flattenedEvent = (String) value.get("flattenedEvent");
            if (flattenedEvent == null) return null;
            JsonNode root = objectMapper.readTree(flattenedEvent);
            JsonNode meta = root.path("eventMetaData");
            return meta.has(fieldName) ? meta.get(fieldName).asText(null) : null;
        } catch (Exception e) {
            return null;
        }
    }
}
