package org.pavani.multithreading.trading_multithreading.dao.hibernate;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.pavani.multithreading.trading_multithreading.config.HibernateConfig;
import org.pavani.multithreading.trading_multithreading.dao.PositionsDAO;
import org.pavani.multithreading.trading_multithreading.entity.Positions;
import org.pavani.multithreading.trading_multithreading.model.Trade;
import org.pavani.multithreading.trading_multithreading.util.database.hibernate.HibernateTransactionUtil;

import java.util.Arrays;
import java.util.logging.Logger;

public class HibernatePositionsDAO implements PositionsDAO {
    SessionFactory factory;
    private static HibernatePositionsDAO instance;
    Logger logger = Logger.getLogger(HibernatePositionsDAO.class.getName());

    public HibernatePositionsDAO() {
        factory = HibernateConfig.getSessionFactory();
    }

    public static synchronized HibernatePositionsDAO getInstance(){
        if (instance == null) {
            instance = new HibernatePositionsDAO();
        }
        return instance;
    }

    public void insertToPositions(Trade trade ) {
        try {
            Session session = HibernateTransactionUtil.getConnection();
            Positions positions = new Positions();
            positions.setAccountNumber(trade.accountNumber());
            positions.setCusip(trade.cusip());
            positions.setPosition(trade.quantity());
            positions.setVersion(0);
            session.persist(positions);
        } catch (Exception e) {
            logger.warning("Stack trace: "+ Arrays.toString(e.getStackTrace()));
        }
    }

    public int updatePositions(Trade trade, int version, int newQuantity) {
        int rowsAffected = 0;
        try {
            Session session = HibernateTransactionUtil.getConnection();
            String hql = "UPDATE Positions p SET p.position = :newQuantity, p.version = :version WHERE p.accountNumber = :accountNumber AND p.cusip = :cusip";
            Query query = session.createQuery(hql);
            query.setParameter("newQuantity", newQuantity);
            query.setParameter("version", version);
            query.setParameter("accountNumber", trade.accountNumber());
            query.setParameter("cusip", trade.cusip());
            rowsAffected = query.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowsAffected;
    }
}

