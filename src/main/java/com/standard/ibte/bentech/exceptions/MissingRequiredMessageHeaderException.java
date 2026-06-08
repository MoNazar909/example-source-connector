package com.standard.ibte.bentech.exceptions;

import java.io.IOException;

public class MissingRequiredMessageHeaderException extends IOException {

    public MissingRequiredMessageHeaderException(String requiredHeaders) {
        super("Missing required message headers: " + requiredHeaders);
    }
}
