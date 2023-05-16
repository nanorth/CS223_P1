package cs223;

import javax.print.DocFlavor;
import java.sql.*;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimulationTest {

    private static String url = "jdbc:postgresql://localhost:5432/testdb";
    private static String user = "postgres";
    private static String password = "en129129";

    // key is timestamp and value is sqls

    public static HashMap<Long, List<String>> sqlMap = new HashMap<>();
    public static List<String> sqlString;

    public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException {
        Settings.switch_to_high_concurrency();
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



        SQLDataLoader.LoadSQL(Settings.OBSERVATION_DATASET_URL, sqlMap);
        SQLDataLoader.LoadSQL(Settings.SEMANTIC_DATASET_URL, sqlMap);
        SQLDataLoader.LoadQueries(Settings.QUERY_DATA_URL, sqlMap);

        for (int i = 0; i < Settings.LEVELS.size(); i++) {
            System.out.println("Isolation Level" + Settings.LEVELS.get(i));
            for (int j = 0; j < Settings.TRANSACTION_SIZE.size(); j++) {
                System.out.println("Transaction size" + Settings.TRANSACTION_SIZE.get(j));
                for (int k = 0; k < Settings.MPLS.size(); k++) {
                    // create connection pool and set isolation level
                    ConnectionPool connectionPool = ConnectionPool.getInstance(Settings.MPLS.get(k), url, user, password);

                    //clean up database
                    sqlString = new ArrayList<>();
                    SQLDataLoader.LoadSQL("Resources/schema/drop.sql", sqlString);
                    executeSql(sqlString, connectionPool, url, user, password);
                    sqlString = new ArrayList<>();
                    SQLDataLoader.LoadSQL("Resources/schema/create.sql", sqlString);
                    executeSql(sqlString, connectionPool, url, user, password);

                    //load matadata
                    sqlString = new ArrayList<>();
                    SQLDataLoader.LoadSQL(Settings.METADATA_DATASET_URL, sqlString);
                    executeSql(sqlString, connectionPool, url, user, password);

                    //load sql settings
                    //executeSql(sqlStatements, connectionPool, url, user, password);

                    int taskCount = (int)Settings.SIMULATION_LENGTH / Settings.PERIOD;
                    int ThreadPoolSize = Settings.MPLS.get(k);
                    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(ThreadPoolSize);
                    QueryScheduler[] schedulerList = new QueryScheduler[ThreadPoolSize];
                    ScheduledFuture<?>[] scheduledFutures = new ScheduledFuture[ThreadPoolSize];
                    CountDownLatch latch = new CountDownLatch(taskCount);
                    long simulationBeginTime = System.currentTimeMillis();

                    for (int temp = 0; temp < ThreadPoolSize; temp++) {
                        //long curSimulationBeginTime = System.currentTimeMillis();
                        schedulerList[temp] = new QueryScheduler(connectionPool,
                                simulationBeginTime, Settings.TRANSACTION_SIZE.get(j), Settings.LEVELS.get(i),
                                temp * Settings.PERIOD, ThreadPoolSize);
                        int finalTemp = temp;
                        scheduledFutures[temp] = scheduler.scheduleWithFixedDelay(
                                () -> {
                                    schedulerList[finalTemp].run();
                                    latch.countDown();
                                }, 0, Settings.PERIOD * ThreadPoolSize, TimeUnit.MILLISECONDS);
                        Thread.sleep(Settings.PERIOD);
                    }


                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
                        scheduledFuture.cancel(true);
                    }
                    scheduler.shutdown();
                    //for test
                    /*List<String> test = new ArrayList<>();
                    test.add("SELECT ci.INFRASTRUCTURE_ID \n" +
                            "FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci \n" +
                            "WHERE sen.id=ci.SENSOR_ID AND sen.id='78dd9081_14a5_41eb_8632_14e45a6b1e57'");
                    executeSql(test, connectionPool, url, user, password);*/

                    long simulationEndTime = System.currentTimeMillis();
                    //TODO: PRINT TRX SIZE, ISO LEVEL, MPL
                    System.out.println("MPL:" + Settings.MPLS.get(k) +" Response time of the whole workload: " +
                            (long)(simulationEndTime - simulationBeginTime));
                    Thread.sleep(10000);
                }
            }
        }


    }



    static class QueryScheduler implements Runnable {
        private ConnectionPool connectionPool;
        private long simulationBeginTime;
        private int transactionSize;
        private int isolationLevel;
        private long counter;
        private int ThreadPoolSize;

        public QueryScheduler(ConnectionPool connectionPool, long simulationBeginTime,
                              int transactionSize, int isolationLevel, long counter, int ThreadPoolSize) {
            this.connectionPool = connectionPool;
            this.simulationBeginTime = simulationBeginTime;
            this.transactionSize = transactionSize;
            this.isolationLevel = isolationLevel;
            this.counter = counter;
            this.ThreadPoolSize = ThreadPoolSize;
        }

        @Override
        public void run() {
            //System.out.println("task schedule at:");
            //System.out.println(System.currentTimeMillis() - simulationBeginTime);
            // get all sql during current time period
            List<String> currentQueries = new ArrayList<>();

            for (long i = counter; i < counter + Settings.PERIOD; i++) {
                List<String> sqls = sqlMap.getOrDefault(i, new ArrayList<>());
                for (String sql : sqls) {
                    currentQueries.add(sql);
                }
            }
            //System.out.println(currentQueries.size());
            counter += Settings.PERIOD * ThreadPoolSize;
            int currentQueryCount = 0;
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                connection = connectionPool.getConnection();
                //System.out.println("task get connection at:");
                //System.out.println(System.currentTimeMillis() - simulationBeginTime);
                connection.setTransactionIsolation(isolationLevel);
                connection.setAutoCommit(false);
                for (String sql : currentQueries) {
                    statement = connection.prepareStatement(sql);
                    if (sql.startsWith("SELECT")) {
                        statement.executeQuery();
                        connection.commit();
                        connection.setAutoCommit(false);
                        continue;
                    }
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
                }
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
                    if (sql.startsWith("SELECT")) {
                        statement.executeQuery(sql);
                        continue;
                    }
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
    //TODO: following code is an example for detecting response time
    /*List<Long> waitTimes = new ArrayList<>();
    for (int i = 0; i < taskCount; i++) {
        long startTime = System.currentTimeMillis();
        Connection conn = dataSource.getConnection();
        long endTime = System.currentTimeMillis();
        waitTimes.add(endTime - startTime);
        // Execute SQL
        // ...
        conn.close();
    }
    double avgWaitTime = waitTimes.stream().mapToLong(Long::longValue).average().orElse(Double.NaN);*/



}

