package org.pavani.multithreading.trading_multithreading.util.database;

public interface TransactionUtil {
    void startTransaction();

    void commitTransaction();

    void rollbackTransaction();
}
