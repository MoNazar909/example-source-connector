package com.example.connectors;

class ApiResponse {
    final int statusCode;
    final String body;
    final int retryCount;

    ApiResponse(int statusCode, String body, int retryCount) {
        this.statusCode = statusCode;
        this.body = body;
        this.retryCount = retryCount;
    }
}
