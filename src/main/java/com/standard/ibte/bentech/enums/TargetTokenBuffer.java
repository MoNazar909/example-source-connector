package com.standard.ibte.bentech.enums;

public enum TargetTokenBuffer {

    PLANSOURCE(180),
    WORKDAY(0),
    BSWIFT(3600);

    private final long bufferSeconds;

    TargetTokenBuffer(long bufferSeconds) {
        this.bufferSeconds = bufferSeconds;
    }

    public long getBufferSeconds() {
        return bufferSeconds;
    }

    public static TargetTokenBuffer forTarget(String targetBentech) {
        for (TargetTokenBuffer t : values()) {
            if (t.name().equalsIgnoreCase(targetBentech)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown targetBentech value: '" + targetBentech
                + "'. Must be one of: WORKDAY, PLANSOURCE, BSWIFT");
    }
}
