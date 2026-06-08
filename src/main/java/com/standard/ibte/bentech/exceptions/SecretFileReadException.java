package com.standard.ibte.bentech.exceptions;

import java.io.IOException;

public class SecretFileReadException extends IOException {

    public SecretFileReadException(String groupId, String suffix, IOException cause) {
        super("Unable to read mounted secrets file '" + groupId + "_" + suffix + "'", cause);
    }
}
