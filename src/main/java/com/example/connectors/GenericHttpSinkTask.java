package com.example.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GenericHttpSinkTask extends SinkTask {

    private HttpClient httpClient;
    private ObjectMapper objectMapper;
    private Map<String, TokenInfo> tokenCache;
    private String secretsBasePath;
    private String contentType;

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

    @Override
    public String version() { return "1.0"; }

    @Override
    public void start(Map<String, String> props) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.tokenCache = new ConcurrentHashMap<>();
        this.secretsBasePath = props.getOrDefault("secrets.base.path", "/etc/secrets");
        this.contentType = props.get("content.type");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                processRecord(record);
            } catch (Exception e) {
                throw new RuntimeException("Failed to process record at offset " + record.kafkaOffset(), e);
            }
        }
    }

    private void processRecord(SinkRecord record) throws Exception {
        Struct value = (Struct) record.value();

        String groupId = (String) value.get("group_id");
        JsonNode flattenedEvent = objectMapper.readTree((String) value.get("flattened_event"));
        String tokenUrl = flattenedEvent.get("target_api_token_url").asText();
        String apiUrl = flattenedEvent.get("target_api_url").asText();
        String payload = (String) value.get("target_HCM_payload");

        System.out.printf("[%s][partition=%d][offset=%d] key=%s groupId=%s apiUrl=%s%n",
            record.topic(), record.kafkaPartition(), record.kafkaOffset(),
            record.key(), groupId, apiUrl);

        String token = getOrRefreshToken(groupId, tokenUrl);
        String response = callApi(apiUrl, token, payload);
        System.out.println("API response: " + response);
    }

    private String getOrRefreshToken(String groupId, String tokenUrl) throws Exception {
        TokenInfo cached = tokenCache.get(groupId);
        if (cached != null && !cached.isExpired()) {
            return cached.accessToken;
        }

        String secretPath = secretsBasePath + "/hcm-connector-tenant-secrets/";
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

        JsonNode json = objectMapper.readTree(response.body());
        String accessToken = json.get("access_token").asText();
        int expiresIn = json.has("expires_in") ? json.get("expires_in").asInt() : 3600;

        tokenCache.put(groupId, new TokenInfo(accessToken, expiresIn));
        System.out.println("Token refreshed for group: " + groupId);
        return accessToken;
    }

    private String callApi(String apiUrl, String token, String payload) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(apiUrl))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", contentType)
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return "HTTP " + response.statusCode() + ": " + response.body();
    }

    private String readSecret(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path))).trim();
    }

    @Override
    public void stop() {}
}
