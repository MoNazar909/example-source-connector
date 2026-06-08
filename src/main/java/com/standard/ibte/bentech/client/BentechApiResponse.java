package com.standard.ibte.bentech.client;

import com.standard.ibte.bentech.enums.ApiResultStatus;

public final class BentechApiResponse {

    public final ApiResultStatus status;
    public final int httpCode;
    public final String responseBody;
    public final String errorMessage;
    public final int retryCount;

    public BentechApiResponse(ApiResultStatus status, int httpCode,
                              String responseBody, String errorMessage, int retryCount) {
        this.status       = status;
        this.httpCode     = httpCode;
        this.responseBody = responseBody;
        this.errorMessage = errorMessage;
        this.retryCount   = retryCount;
    }
}
