package org.pavani.multithreading.trading_multithreading.dao;

import org.pavani.multithreading.trading_multithreading.model.Trade;

import java.sql.SQLException;

public interface JournalEntryDAO {
    void insertToJournalEntry(Trade trade) throws SQLException;
}
