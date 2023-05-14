package cs223;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.sun.xml.internal.ws.api.config.management.policy.ManagementAssertion;
import org.apache.commons.dbcp2.BasicDataSource;

public class ConnectionPool {

    private static ConnectionPool instance = null;
    private static BasicDataSource dataSource = null;

    private ConnectionPool() {
    }

    public static synchronized ConnectionPool getInstance(int MPL) {
        if (instance == null) {
            instance = new ConnectionPool();
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl("jdbc:mysql://localhost:3306/mydb");
            dataSource.setUsername("root");
            dataSource.setPassword("password");
            dataSource.setInitialSize((int)MPL / 4);
            dataSource.setMaxTotal(MPL);
            dataSource.setMaxIdle((int)MPL / 2);
            dataSource.setMinIdle((int)MPL / 4);
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void close() throws SQLException {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}

