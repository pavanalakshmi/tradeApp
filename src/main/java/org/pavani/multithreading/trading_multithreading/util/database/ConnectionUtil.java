package org.pavani.multithreading.trading_multithreading.util.database;

public interface ConnectionUtil<T> {
    T getConnection();
}