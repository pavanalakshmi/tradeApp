package org.pavani.multithreading.trading_multithreading.dao.hibernate;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.pavani.multithreading.trading_multithreading.dao.JournalEntryDAO;
import org.pavani.multithreading.trading_multithreading.dao.PayloadDAO;
import org.pavani.multithreading.trading_multithreading.entity.JournalEntry;
import org.pavani.multithreading.trading_multithreading.factory.BeanFactory;
import org.pavani.multithreading.trading_multithreading.model.Trade;
import org.pavani.multithreading.trading_multithreading.util.database.hibernate.HibernateTransactionUtil;

import java.sql.SQLException;

// DAO ~ repository

public class HibernateJournalEntryDAO implements JournalEntryDAO {
    private static final SessionFactory factory = new Configuration()
            .configure("hibernate.cfg.xml")
            .addAnnotatedClass(JournalEntry.class)
            .buildSessionFactory();
    private static HibernateJournalEntryDAO instance;
    private static final PayloadDAO payloadDAO = BeanFactory.getPayloadDAO();

    private HibernateJournalEntryDAO() {
    }

    public static HibernateJournalEntryDAO getInstance(){
        if (instance == null) {
            instance = new HibernateJournalEntryDAO();
        }
        return instance;
    }

    public void insertToJournalEntry(Trade trade) throws SQLException{
        try {
            Session session = HibernateTransactionUtil.getConnection();
            JournalEntry journalEntry = new JournalEntry();
            journalEntry.setAccountNumber(trade.accountNumber());
            journalEntry.setCusip(trade.cusip());
            journalEntry.setDirection(trade.direction());
            journalEntry.setQuantity(trade.quantity());

            session.persist(journalEntry);
            payloadDAO.updatePayload(trade.tradeId(), "posted");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
