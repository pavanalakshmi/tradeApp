package org.pavani.multithreading.trading_multithreading.exception;

public class HikariCPConnectionException extends RuntimeException {
    public HikariCPConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
