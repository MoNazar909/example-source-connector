package com.example.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class WorkdayApiClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Map<String, TokenInfo> tokenCache;
    private final String secretsBasePath;
    private final String contentType;
    private final int maxRetries;
    private final long retryBackoffMs;

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

    WorkdayApiClient(String secretsBasePath, String contentType, int maxRetries, long retryBackoffMs) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.tokenCache = new ConcurrentHashMap<>();
        this.secretsBasePath = secretsBasePath;
        this.contentType = contentType;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    ApiResponse callApiWithRetry(String apiUrl, String token, String payload) throws Exception {
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
            String body = httpResponse.body();

            boolean retryable = (statusCode == 429 || statusCode >= 500) && !isSoapFault(body);
            if (!retryable || attempts >= maxRetries) {
                return new ApiResponse(statusCode, body, attempts);
            }

            attempts++;
            System.out.printf("Transient HTTP %d, retry %d/%d%n", statusCode, attempts, maxRetries);
            Thread.sleep(retryBackoffMs);
        }
    }

    String getOrRefreshToken(String groupId, String tokenUrl) throws Exception {
        TokenInfo cached = tokenCache.get(groupId);
        if (cached != null && !cached.isExpired()) {
            return cached.accessToken;
        }

        String secretPath = secretsBasePath + "/bentech-connector-tenant-secrets/";
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

        JsonNode json      = objectMapper.readTree(response.body());
        String accessToken = json.get("access_token").asText();
        int expiresIn      = json.has("expires_in") ? json.get("expires_in").asInt() : 3600;

        tokenCache.put(groupId, new TokenInfo(accessToken, expiresIn));
        System.out.println("Token refreshed for group: " + groupId);
        return accessToken;
    }

    private boolean isSoapFault(String body) {
        return body != null && (body.contains("<Fault>") || body.contains(":Fault>"));
    }

    private String readSecret(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path))).trim();
    }
}
