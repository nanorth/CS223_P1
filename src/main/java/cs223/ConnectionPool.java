package cs223;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

public class ConnectionPool {

    private static ConnectionPool instance = null;
    public static BasicDataSource dataSource = null;

    private ConnectionPool() {
    }

    public static synchronized ConnectionPool getInstance(int MPL, String url, String user, String password) {
        if (instance == null) {
            instance = new ConnectionPool();
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName("org.postgresql.Driver");
            dataSource.setUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSource.setInitialSize(0);
            dataSource.setMaxTotal(MPL);
            dataSource.setMaxIdle(0);
            dataSource.setMinIdle(0);
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void close() throws SQLException {
        if (dataSource != null) {
            dataSource.close();
            instance = null;
            dataSource = null;
        }
    }
}

