package com.standard.ibte.bentech.client;

import java.time.Instant;

public final class TokenEntry {

    volatile String accessToken;
    volatile String refreshToken;
    volatile Instant tokenExpiry = Instant.EPOCH;
}
