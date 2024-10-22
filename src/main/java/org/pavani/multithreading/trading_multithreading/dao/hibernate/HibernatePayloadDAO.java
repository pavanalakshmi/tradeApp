package org.pavani.multithreading.trading_multithreading.dao.hibernate;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.pavani.multithreading.trading_multithreading.config.HibernateConfig;
import org.pavani.multithreading.trading_multithreading.dao.PayloadDAO;
import org.pavani.multithreading.trading_multithreading.dao.ReadPayloadDAO;
import org.pavani.multithreading.trading_multithreading.dao.RetrieveJournalEntryDAO;
import org.pavani.multithreading.trading_multithreading.entity.TradePayloads;
import org.pavani.multithreading.trading_multithreading.util.database.hibernate.HibernateTransactionUtil;

import java.sql.SQLException;
import java.util.logging.Logger;

public class HibernatePayloadDAO implements PayloadDAO {
    ReadPayloadDAO readPayloadDAO;
    RetrieveJournalEntryDAO retrieveJournalEntryDAO;
    SessionFactory factory;
    private static HibernatePayloadDAO instance;
    Logger logger = Logger.getLogger(HibernatePayloadDAO.class.getName());

    public HibernatePayloadDAO() {
        readPayloadDAO = new ReadPayloadDAO();
        retrieveJournalEntryDAO = new RetrieveJournalEntryDAO();
        factory = HibernateConfig.getSessionFactory();
    }

    public static synchronized HibernatePayloadDAO getInstance(){
        if (instance == null) {
            instance = new HibernatePayloadDAO();
        }
        return instance;
    }

    public void insertIntoPayload(String line) throws SQLException {
        String[] data = line.split(",");
        String status = checkValidPayloadStatus(data) ? "valid" : "invalid";
        boolean validCusip = readPayloadDAO.isValidCUSIPSymbolHibernate(data[3]);
        String lookUpStatus = validCusip ? "pass" : "fail";
        boolean journalEntryStatus = retrieveJournalEntryDAO.isJournalEntryExistHibernate(data[2], data[3]);
        String jeStatus = journalEntryStatus ? "posted" : "not_posted";

        try {
            Session session = HibernateTransactionUtil.getConnection();

            TradePayloads tradePayloads = new TradePayloads();
            tradePayloads.setTradeId(data[0]);
            tradePayloads.setValidityStatus(status);
            tradePayloads.setPayload(line);
            tradePayloads.setLookupStatus(lookUpStatus);
            tradePayloads.setJeStatus(jeStatus);

            session.persist(tradePayloads);

        } catch (Exception e) {
            System.out.println("Error while inserting row: " + e.getMessage());
        }
    }

    public void updatePayload(String tradeId, String newStatus) throws SQLException {
        try {
            Session session = HibernateTransactionUtil.getConnection();
            String hql = "UPDATE TradePayloads SET jeStatus = :newStatus WHERE tradeId = :tradeId";
            Query query = session.createQuery(hql);
            query.setParameter("newStatus", newStatus);
            query.setParameter("tradeId", tradeId);
            int result = query.executeUpdate();
            System.out.println("Rows affected: " + result);
        } catch (Exception e) {
            System.out.println("Error updating row: " + e.getMessage());
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

