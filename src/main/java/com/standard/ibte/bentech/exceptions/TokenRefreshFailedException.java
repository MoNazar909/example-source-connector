package com.standard.ibte.bentech.exceptions;

public class TokenRefreshFailedException extends Exception {

    public TokenRefreshFailedException(String groupId, int statusCode, String responseBody) {
        super("Token refresh failed for group '" + groupId + "': HTTP " + statusCode + " - " + responseBody);
    }

    public TokenRefreshFailedException(String groupId, String reason, Throwable cause) {
        super("Token refresh failed for group '" + groupId + "': " + reason, cause);
    }
}
