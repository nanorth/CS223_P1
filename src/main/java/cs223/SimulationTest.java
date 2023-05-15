package cs223;

import javax.print.DocFlavor;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;


public class SimulationTest {

    private static String url = "jdbc:postgresql://localhost:5432/testdb";
    private static String user = "postgres";
    private static String password = "en129129";

    // key is timestamp and value is sqls
    // TODO: LOAD SQLS!
    public static HashMap<Long, List<String>> sqlMap = new HashMap<>();
    public static List<String> sqlString = new ArrayList<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException {


        ArrayList<String> sqlStatements = new ArrayList<String>();
        sqlStatements.add("SET statement_timeout = 0;");
        sqlStatements.add("SET lock_timeout = 0;");
        sqlStatements.add("SET idle_in_transaction_session_timeout = 0;");
        sqlStatements.add("SET client_encoding = 'UTF8';");
        sqlStatements.add("SET standard_conforming_strings = on;");
        sqlStatements.add("SET check_function_bodies = false;");
        sqlStatements.add("SET client_min_messages = warning;");
        sqlStatements.add("SET row_security = off;");
        sqlStatements.add("SET search_path = public, pg_catalog;");

        //load matadata
        ConnectionPool connectionPool = ConnectionPool.getInstance(1, url, user, password);
        sqlString = new ArrayList<>();
        SQLDataLoader.LoadSQL("Resources/data/low_concurrency/metadata.sql", sqlString);
        executeSql(sqlString, connectionPool, url, user, password);

        SQLDataLoader.LoadSQL(Settings.OBSERVATION_DATASET_URL, sqlMap);

        for (int i = 0; i < Settings.LEVELS.size(); i++) {
            for (int j = 0; j < Settings.TRANSACTION_SIZE.size(); j++) {
                for (int k = 0; k < Settings.MPLS.size(); k++) {

                    // create connection pool and set isolation level
                    connectionPool = ConnectionPool.getInstance(Settings.MPLS.get(k), url, user, password);

                    //clean up database
                    SQLDataLoader.LoadSQL("Resources/schema/drop.sql", sqlString);
                    executeSql(sqlString, connectionPool, url, user, password);
                    sqlString = new ArrayList<>();
                    SQLDataLoader.LoadSQL("Resources/schema/create.sql", sqlString);
                    executeSql(sqlString, connectionPool, url, user, password);

                    //load sql settings
                    executeSql(sqlStatements, connectionPool, url, user, password);


                    // create scheduled task
                    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                    long simulationBeginTime = System.currentTimeMillis();
                    // execute the task and get the ScheduledFuture instance
                    QueryScheduler queryScheduler = new QueryScheduler(connectionPool,
                            simulationBeginTime, Settings.TRANSACTION_SIZE.get(j), Settings.LEVELS.get(i));
                    ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(
                            queryScheduler, 0, Settings.PERIOD, TimeUnit.MILLISECONDS);

                    // wait for the task to complete
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    scheduledFuture.cancel(true);

                    long simulationEndTime = System.currentTimeMillis();
                    System.out.println("Response time of the whole workload: " +
                            (long)(simulationEndTime - simulationBeginTime));
                    Thread.sleep(1000);
                }
            }
        }


    }



    static class QueryScheduler implements Runnable {
        private ConnectionPool connectionPool;
        private long simulationBeginTime;
        private int transactionSize;
        private int isolationLevel;

        public QueryScheduler(ConnectionPool connectionPool, long simulationBeginTime,
                              int transactionSize, int isolationLevel) {
            this.connectionPool = connectionPool;
            this.simulationBeginTime = simulationBeginTime;
            this.transactionSize = transactionSize;
            this.isolationLevel = isolationLevel;
        }

        @Override
        public void run() {
            long time = System.currentTimeMillis();
            // get all sql during current time period
            List<String> currentQueries = new ArrayList<>();
            for (int i = 0; i < Settings.PERIOD; i++) {
                List<String> sqls = sqlMap.getOrDefault(time + i - simulationBeginTime, new ArrayList<>());
                for (String sql : sqls) {
                    currentQueries.add(sql);
                }
            }
            int currentQueryCount = 0;
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                connection = connectionPool.getConnection();
                connection.setTransactionIsolation(isolationLevel);
                connection.setAutoCommit(false);

                for (String sql : currentQueries) {
                    statement = connection.prepareStatement(sql);
                    statement.executeUpdate();

                    currentQueryCount++;

                    if (currentQueryCount % transactionSize == 0) {
                        connection.commit();
                        connection.setAutoCommit(false);
                    }
                }

                connection.commit();
            } catch (SQLException e) {
                try {
                    if (connection != null) {
                        connection.rollback();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            } finally {
                try {
                    if (connection != null) {
                        connection.setAutoCommit(true);
                        connection.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


        }
    }


    public static void executeSql(List<String> sqlStatements, ConnectionPool connectionPool, String url, String user, String password) {
        // set connection
        Connection connection = null;
        try {
            connection = connectionPool.getConnection();
            try (Statement statement = connection.createStatement()) {
                for (String sql : sqlStatements) {
                    statement.executeUpdate(sql);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}

