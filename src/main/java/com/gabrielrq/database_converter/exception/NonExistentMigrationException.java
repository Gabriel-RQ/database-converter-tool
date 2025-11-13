package com.gabrielrq.database_converter.exception;

public class NonExistentMigrationException extends RuntimeException {
    public NonExistentMigrationException(String message) {
        super(message);
    }
}
