package org.pavani.multithreading.trading_multithreading.util.database.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.pavani.multithreading.trading_multithreading.exception.HikariCPConnectionException;
import org.pavani.multithreading.trading_multithreading.exception.TransactionHandlingException;
import org.pavani.multithreading.trading_multithreading.util.ApplicationConfigProperties;
import org.pavani.multithreading.trading_multithreading.util.database.ConnectionUtil;
import org.pavani.multithreading.trading_multithreading.util.database.TransactionUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;


public class JDBCTransactionUtil implements TransactionUtil, ConnectionUtil<Connection> {
    private DataSource dataSource;
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<>();
    private static JDBCTransactionUtil instance;
    private static final ApplicationConfigProperties applicationConfigProperties = ApplicationConfigProperties.getInstance();
    private static final String DB_USERNAME = applicationConfigProperties.getDbUserName();
    private static final String DB_PASSWORD = applicationConfigProperties.getDbPasswords();
    private static final String DB_URL = applicationConfigProperties.getDbUrl();
    Logger logger = Logger.getLogger(JDBCTransactionUtil.class.getName());

    private JDBCTransactionUtil() {
        // private constructor to prevent instantiation
    }

    public static synchronized JDBCTransactionUtil getInstance() {
        if (instance == null) {
            instance = new JDBCTransactionUtil();
        }
        return instance;
    }

    @Override
    public Connection getConnection() {
        Connection connection = connectionHolder.get();
        if (connection == null) {
            dataSource = getHikariDataSource();
            try {
                connection = dataSource.getConnection();
                connectionHolder.set(connection);
            } catch (Exception e) {
                e.printStackTrace();
                throw new HikariCPConnectionException("Error getting connection from HikariCP", e);
            }
        }
        return connection;
    }

    private synchronized DataSource getHikariDataSource() {
        if (dataSource == null) {
            createDataSource();
        }
        // Configure the HikariCP connection pool
        return dataSource;
    }

    private void createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL); //bootcamp
        config.setUsername(DB_USERNAME);
        config.setPassword(DB_PASSWORD);
        config.setMaximumPoolSize(50); // Set max connections in pool
        config.setConnectionTimeout(30000); // Timeout in milliseconds
        config.setIdleTimeout(600000); // Idle timeout before connection is closed
        // Create the HikariCP data source
        dataSource = new HikariDataSource(config);
    }

    @Override
    public void startTransaction() {
        try {
            getConnection().setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void closeConnection() {
        Connection connection = connectionHolder.get();
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connectionHolder.remove();
            }
        }
    }

    @Override
    public void commitTransaction() {
        try {
            connectionHolder.get().commit();
            connectionHolder.get().setAutoCommit(false);
            closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new TransactionHandlingException("error committing transaction", e);
        }
    }

    @Override
    public void rollbackTransaction() {
        try {
            connectionHolder.get().rollback();
            connectionHolder.get().setAutoCommit(false);
            closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new TransactionHandlingException("error rolling back transaction", e);
        }
    }
}
