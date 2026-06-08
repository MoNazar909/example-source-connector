package com.standard.ibte.bentech.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.standard.ibte.bentech.BentechSinkConfig;
import com.standard.ibte.bentech.BentechSinkRecordMapper;
import com.standard.ibte.bentech.enums.ApiResultStatus;
import com.standard.ibte.bentech.enums.TargetTokenBuffer;
import com.standard.ibte.bentech.exceptions.MissingRequiredMessageHeaderException;
import com.standard.ibte.bentech.exceptions.TokenRefreshFailedException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Sends SinkRecords to the Bentech API.
 * Extracts API URL, token URL, and group_id from the Avro record value.
 * Token acquisition and caching is delegated to BentechTokenManager.
 */
public final class BentechClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BentechClient.class);

    private final String requestContentType;
    private final long tokenRefreshBufferSeconds;
    private final BentechSinkRecordMapper mapper;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final BentechTokenManager tokenManager;

    public BentechClient(BentechSinkConfig config) {
        this.requestContentType       = config.getString(BentechSinkConfig.CONNECTOR_REQUEST_CONTENT_TYPE);
        this.tokenRefreshBufferSeconds = config.getInt(BentechSinkConfig.CONNECTOR_TOKEN_REFRESH_BUFFER_SECONDS);
        this.mapper                   = new BentechSinkRecordMapper();
        this.httpClient               = HttpClient.newHttpClient();
        this.objectMapper             = new ObjectMapper();
        this.tokenManager             = new BentechTokenManager(
                config.getString(BentechSinkConfig.CONNECTOR_SECRETS_MOUNT_PATH),
                httpClient, objectMapper);
    }

    /**
     * Processes a single SinkRecord:
     * 1. Extracts api URL, token URL, group_id, and target_bentech from the record value
     * 2. Acquires / reuses the cached access token via BentechTokenManager
     * 3. Extracts and unescapes the SOAP XML from the target_bentech_payload field
     * 4. POSTs the XML to the API with Bearer auth
     * 5. Returns a BentechApiResponse classifying the outcome:
     *    SUCCESS     – HTTP 2xx
     *    DATA_ERROR  – HTTP 4xx, or HTTP 5xx with a SOAP Fault body (no retry)
     *    SYSTEM_ERROR – HTTP 5xx without a SOAP Fault (transient; task will retry)
     */
    public BentechApiResponse sendRecord(SinkRecord record) throws IOException {
        Struct value = (Struct) record.value();

        String groupId    = (String) value.get("groupId");
        String apiUrl     = mapper.extractApiUrl(value);
        String tokenUrl   = mapper.extractTokenUrl(value);
        String targetBentech = resolveBentechType(value);

        if (apiUrl == null || tokenUrl == null || groupId == null) {
            throw new MissingRequiredMessageHeaderException("groupId, targetHostname, targetTokenHostname");
        }

        long bufferSeconds = resolveBufferSeconds(targetBentech);

        String accessToken;
        try {
            accessToken = tokenManager.ensureToken(groupId, tokenUrl, bufferSeconds, targetBentech);
        } catch (TokenRefreshFailedException e) {
            throw new IOException("Failed to acquire access token for group '" + groupId + "'", e);
        }

        String soapXml = mapper.extractSoapPayload(record);

        log.debug("Posting payload to API – group: {}, url: {}, topic: {}, offset: {}",
                groupId, apiUrl, record.topic(), record.kafkaOffset());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Authorization", "Bearer " + accessToken)
                .header("Content-Type", requestContentType)
                .POST(HttpRequest.BodyPublishers.ofString(soapXml))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int code = response.statusCode();
            String body = response.body() != null ? response.body() : "";

            if (code >= 200 && code < 300) {
                log.info("API call succeeded – group: {}, HTTP: {}, topic: {}, offset: {}",
                        groupId, code, record.topic(), record.kafkaOffset());
                return new BentechApiResponse(ApiResultStatus.SUCCESS, code, body, null, 0);
            }

            boolean isSoapFault = body.contains("<Fault>") || body.contains(":Fault>");

            if (code < 500 || isSoapFault) {
                log.warn("Data error from API – group: {}, HTTP: {}, soapFault: {}, topic: {}, offset: {}",
                        groupId, code, isSoapFault, record.topic(), record.kafkaOffset());
                return new BentechApiResponse(ApiResultStatus.DATA_ERROR, code, body,
                        "HTTP " + code + " - " + body, 0);
            }

            log.warn("System error from API – group: {}, HTTP: {}, topic: {}, offset: {}",
                    groupId, code, record.topic(), record.kafkaOffset());
            return new BentechApiResponse(ApiResultStatus.SYSTEM_ERROR, code, body,
                    "HTTP " + code + " - " + body, 0);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP call interrupted for group '" + groupId + "'", e);
        }
    }

    /**
     * Determines the bentech type from the targetBentech field in the Avro Struct value,
     * defaulting to WORKDAY if the field is absent from the schema.
     */
    private String resolveBentechType(Struct value) {
        try {
            Object field = value.get("targetBentech");
            if (field != null) return field.toString();
        } catch (Exception ignored) {
            // field not present in current schema version
        }
        return "WORKDAY";
    }

    private long resolveBufferSeconds(String targetBentech) {
        try {
            return TargetTokenBuffer.forTarget(targetBentech).getBufferSeconds();
        } catch (IllegalArgumentException e) {
            return tokenRefreshBufferSeconds;
        }
    }

    @Override
    public void close() {
        // HttpClient does not require explicit close
    }
}
