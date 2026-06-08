package com.standard.ibte.bentech.exceptions;

import java.io.IOException;

public class InvalidBentechTypeException extends IOException {

    public InvalidBentechTypeException(String bentechTypeName) {
        super("Unsupported Bentech type '" + bentechTypeName + "'");
    }
}
