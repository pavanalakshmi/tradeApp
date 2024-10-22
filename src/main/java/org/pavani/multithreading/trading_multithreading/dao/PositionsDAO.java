package org.pavani.multithreading.trading_multithreading.dao;

import org.pavani.multithreading.trading_multithreading.model.Trade;

import java.sql.Connection;
import java.sql.SQLException;

public interface PositionsDAO {
    void insertToPositions(Trade trade);
    int updatePositions(Trade trade, int version, int newQuantity) throws SQLException;
    }
