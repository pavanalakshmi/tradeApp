package org.pavani.multithreading.trading_multithreading.dao;

import java.sql.SQLException;

public interface PayloadDAO {
    void insertIntoPayload(String line) throws SQLException;
    void updatePayload(String tradeId, String newStatus) throws SQLException;
}
