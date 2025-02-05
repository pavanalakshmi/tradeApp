package org.pavani.multithreading.trading_multithreading.dao;

import org.pavani.multithreading.trading_multithreading.model.Trade;
import org.pavani.multithreading.trading_multithreading.util.database.jdbc.JDBCTransactionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RetrievePositionsDataDAO {
    public int getQuantityFromPositions(Trade trade) {
        Connection connection = JDBCTransactionUtil.getInstance().getConnection();
        String selectSQL = "SELECT position FROM positions where account_number = ? AND CUSIP = ?";
        try (PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
            selectStatement.setString(1, trade.accountNumber());
            selectStatement.setString(2, trade.cusip());
            ResultSet rs = selectStatement.executeQuery();
            if(rs.next()){
                return  rs.getInt("position");
            }
            else{
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int getVersionFromPositions(Trade trade) {
        String selectSQL = "SELECT version FROM positions where account_number = ? and CUSIP = ?";
        Connection connection = JDBCTransactionUtil.getInstance().getConnection();
        try (PreparedStatement selectStatement = connection.prepareStatement(selectSQL)) {
            selectStatement.setString(1, trade.accountNumber());
            selectStatement.setString(2, trade.cusip());
            ResultSet rs = selectStatement.executeQuery();
            if(rs.next()){
                return  rs.getInt("version");
            }
            else{
                return -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
