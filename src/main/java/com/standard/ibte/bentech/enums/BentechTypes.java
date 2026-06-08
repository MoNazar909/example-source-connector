package com.standard.ibte.bentech.enums;

import com.standard.ibte.bentech.exceptions.InvalidBentechTypeException;

public enum BentechTypes {

    //                         requiresRequestHeader  requiresRefreshToken
    WORKDAY(false, true),   // OAuth2 refresh_token grant; refresh token read from secrets
    PLANSOURCE(false, false), // client_credentials via JWT assertion; no refresh token needed
    BSWIFT(true, false);    // client_credentials with Basic Auth header; no refresh token needed

    private final boolean requiresRequestHeader;
    private final boolean requiresRefreshToken;

    BentechTypes(boolean requiresRequestHeader, boolean requiresRefreshToken) {
        this.requiresRequestHeader = requiresRequestHeader;
        this.requiresRefreshToken  = requiresRefreshToken;
    }

    public boolean requiresRequestHeader() {
        return requiresRequestHeader;
    }

    public boolean requiresRefreshToken() {
        return requiresRefreshToken;
    }

    public static BentechTypes validate(String value) throws InvalidBentechTypeException {
        for (BentechTypes type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new InvalidBentechTypeException(value);
    }
}
