package com.standard.ibte.bentech.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.standard.ibte.bentech.enums.BentechTypes;
import com.standard.ibte.bentech.exceptions.InvalidBentechTypeException;
import com.standard.ibte.bentech.exceptions.SecretFileReadException;
import com.standard.ibte.bentech.exceptions.TokenRefreshFailedException;

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
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages OAuth access tokens for the Bentech API per group_id.
 * Caches tokens in memory and refreshes proactively based on expiry buffer.
 * Per-group locks prevent thundering-herd on concurrent refresh.
 */
public final class BentechTokenManager {

    private final String secretsBasePath;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, TokenEntry> tokenCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> groupLocks    = new ConcurrentHashMap<>();

    public BentechTokenManager(String secretsBasePath, HttpClient httpClient, ObjectMapper objectMapper) {
        this.secretsBasePath = secretsBasePath;
        this.httpClient      = httpClient;
        this.objectMapper    = objectMapper;
    }

    /**
     * Returns a valid access token for the given groupId, refreshing if necessary.
     * Token rotation: a new refresh_token from the response replaces the cached one.
     * Effective cache duration is min(expires_in, cacheDurationMinutes * 60) - bufferSeconds.
     */
    public String ensureToken(String groupId, String tokenUrl,
                              long cacheDurationMinutes, long tokenRefreshBufferSeconds, String targetBentech)
            throws TokenRefreshFailedException {

        TokenEntry entry = tokenCache.computeIfAbsent(groupId, k -> new TokenEntry());

        // Fast path – cached token still valid (buffer baked in)
        if (entry.accessToken != null && Instant.now().isBefore(entry.tokenExpiry)) {
            return entry.accessToken;
        }

        // Slow path – acquire per-group lock to avoid thundering herd
        synchronized (getLockForGroup(groupId)) {
            // Double-checked locking
            if (entry.accessToken != null && Instant.now().isBefore(entry.tokenExpiry)) {
                return entry.accessToken;
            }

            try {
                BentechTypes bentechType = BentechTypes.validate(targetBentech);

                String[] creds = readClientCredentials(groupId);
                String clientId     = creds[0];
                String clientSecret = creds[1];

                // Bootstrap refresh token from file on first use (Workday only); rotated from response thereafter
                if (entry.refreshToken == null && bentechType.requiresRefreshToken()) {
                    entry.refreshToken = creds[2];
                }

                String requestBody = buildRequestBody(entry.refreshToken, clientId, clientSecret, bentechType);

                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(tokenUrl))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody));

                if (bentechType.requiresRequestHeader()) {
                    requestBuilder.header("Authorization", buildBasicAuthHeader(clientId, clientSecret));
                }

                HttpResponse<String> response = httpClient.send(
                        requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    throw new TokenRefreshFailedException(groupId, response.statusCode(), response.body());
                }

                JsonNode json     = objectMapper.readTree(response.body());
                entry.accessToken = json.get("access_token").asText();
                int expiresIn     = json.has("expires_in") ? json.get("expires_in").asInt() : 3600;

                // If the provider issued a new refresh token, rotate it
                if (json.has("refresh_token") && !json.get("refresh_token").isNull()) {
                    entry.refreshToken = json.get("refresh_token").asText();
                }

                long cacheDurationSeconds = Math.min(expiresIn, cacheDurationMinutes * 60L);
                entry.tokenExpiry = Instant.now().plusSeconds(cacheDurationSeconds - tokenRefreshBufferSeconds);

            } catch (InvalidBentechTypeException e) {
                throw new TokenRefreshFailedException(groupId, "invalid Bentech type: " + e.getMessage(), e);
            } catch (SecretFileReadException e) {
                throw new TokenRefreshFailedException(groupId, "secret file error: " + e.getMessage(), e);
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                throw new TokenRefreshFailedException(groupId, "HTTP communication error: " + e.getMessage(), e);
            }

            return entry.accessToken;
        }
    }

    private Object getLockForGroup(String groupId) {
        return groupLocks.computeIfAbsent(groupId, k -> new Object());
    }

    /**
     * Reads the colon-separated credential file for a group.
     * Convention: {secretsBasePath}/bentech-tenant-creds/client-creds-{groupId}
     * File content: clientId:clientSecret:refreshToken
     */
    private String[] readClientCredentials(String groupId) throws SecretFileReadException {
        String filePath = secretsBasePath + "/bentech-tenant-creds/client-creds-" + groupId;
        try {
            String content = Files.readString(Paths.get(filePath)).trim();
            return content.split(":", 3);
        } catch (IOException e) {
            throw new SecretFileReadException(filePath, e);
        }
    }

    private String buildRequestBody(String refreshToken, String clientId,
                                    String clientSecret, BentechTypes bentechType)
            throws InvalidBentechTypeException {
        switch (bentechType) {
            case WORKDAY:
                return "grant_type=refresh_token"
                        + "&refresh_token=" + urlEncode(refreshToken)
                        + "&client_id="     + urlEncode(clientId)
                        + "&client_secret=" + urlEncode(clientSecret);
            case PLANSOURCE:
                return "client_assertion=" + urlEncode(clientSecret)
                        + "&client_id_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
                        + "&client_id=the_standard_eoi_decisions"
                        + "&grant_type=client_credentials";
            case BSWIFT:
                return "grant_type=client_credentials";
            default:
                throw new InvalidBentechTypeException(bentechType.name());
        }
    }

    private String buildBasicAuthHeader(String clientId, String clientSecret) {
        String credentials = clientId + ":" + clientSecret;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
