package org.pavani.multithreading.trading_multithreading.dao.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.pavani.multithreading.trading_multithreading.config.HikariCPConfig;
import org.pavani.multithreading.trading_multithreading.dao.PayloadDAO;
import org.pavani.multithreading.trading_multithreading.dao.ReadPayloadDAO;
import org.pavani.multithreading.trading_multithreading.dao.RetrieveJournalEntryDAO;
import org.pavani.multithreading.trading_multithreading.service.TradePayload;
import org.pavani.multithreading.trading_multithreading.util.database.jdbc.JDBCTransactionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

public class JDBCPayloadDAO implements PayloadDAO {
    HikariDataSource dataSource;
    ReadPayloadDAO readPayloadDAO;
    RetrieveJournalEntryDAO retrieveJournalEntryDAO;
    String insertSQL = "INSERT INTO trade_payloads(trade_id, validity_status, payload, lookup_status, je_status) VALUES (?,?,?,?,?)";
    SessionFactory factory;
    private static JDBCPayloadDAO instance;
    Logger logger = Logger.getLogger(JDBCPayloadDAO.class.getName());

    public JDBCPayloadDAO() {
        readPayloadDAO = new ReadPayloadDAO();
        dataSource = HikariCPConfig.getDataSource();
        retrieveJournalEntryDAO = new RetrieveJournalEntryDAO();
        factory = new Configuration().configure("hibernate.cfg.xml").addAnnotatedClass(TradePayload.class).buildSessionFactory();
    }

    public static synchronized JDBCPayloadDAO getInstance(){
        if (instance == null) {
            instance = new JDBCPayloadDAO();
        }
        return instance;
    }

    public void insertIntoPayload(String line) {
        String[] data = line.split(",");
        String status = checkValidPayloadStatus(data) ? "valid" : "invalid";
        boolean validCusip = readPayloadDAO.isValidCUSIPSymbol(data[3]);
        String lookUpStatus = validCusip ? "pass" : "fail";
        boolean journalEntryStatus = retrieveJournalEntryDAO.isJournalEntryExist(data[2], data[3]);
        String jeStatus = journalEntryStatus ? "posted" : "not_posted";

        Connection connection = JDBCTransactionUtil.getInstance().getConnection();
        try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {
            insertStatement.setString(1, data[0]);
            insertStatement.setString(2, status);
            insertStatement.setString(3, line);
            insertStatement.setString(4, lookUpStatus);
            insertStatement.setString(5, jeStatus);
            insertStatement.executeUpdate();
        }
        catch (SQLException e) {
            logger.warning("Error processing row: " + e.getMessage());
        }
    }

    public void updatePayload(String tradeId, String newStatus){
        String updateSQL = "UPDATE trade_payloads SET je_status = ? WHERE trade_id = ?";
        Connection connection = JDBCTransactionUtil.getInstance().getConnection();
        try (PreparedStatement updateStatement = connection.prepareStatement(updateSQL)) {
            updateStatement.setString(1, newStatus);
            updateStatement.setString(2, tradeId);
            updateStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Error updating row: " + e.getMessage());
        }
    }

    private boolean checkValidPayloadStatus(String[] data) {
        if (data.length != 7) {
            return false;
        }
        for(String s:data){
            if(s == null || s.trim().isEmpty()){
                return false;
            }
        }
        return true;
    }
}

