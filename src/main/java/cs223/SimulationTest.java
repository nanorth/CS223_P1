package cs223;

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

    public static HashMap<Long, List<String>> sqlMap = new HashMap<>();
    public static List<String> sqlString;
    public static int finishedTask;

    public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException {
        Settings.switch_to_high_concurrency();

        SQLDataLoader.LoadSQL(Settings.OBSERVATION_DATASET_URL, sqlMap);
        SQLDataLoader.LoadSQL(Settings.SEMANTIC_DATASET_URL, sqlMap);
        SQLDataLoader.LoadQueries(Settings.QUERY_DATA_URL, sqlMap);


        for (long i = 0; i < Settings.SIMULATION_LENGTH; i++) {
            if (sqlMap.containsKey(i))
                Statistic.sqlSize += sqlMap.get(i).size();
        }



        System.out.println("sqlSize: " + Statistic.sqlSize);
        for (int i = 0; i < Settings.LEVELS.size(); i++) {
            System.out.println("Isolation Level" + Settings.LEVELS.get(i));
            for (int j = 0; j < Settings.TRANSACTION_SIZE.size(); j++) {
                System.out.println("Transaction size" + Settings.TRANSACTION_SIZE.get(j));
                for (int k = 0; k < Settings.MPLS.size(); k++) {
                    Statistic.totalResponseTime = 0;
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
                    int ThreadPoolSize = taskCount;
                    finishedTask = taskCount;
                    //ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(ThreadPoolSize);
                    QueryScheduler[] schedulerList = new QueryScheduler[ThreadPoolSize];
                    //ScheduledFuture<?>[] scheduledFutures = new ScheduledFuture[ThreadPoolSize];
                    //CountDownLatch latch = new CountDownLatch(taskCount);
                    long simulationBeginTime = System.currentTimeMillis();

                    /*QueryScheduler scheduler = new QueryScheduler(connectionPool,
                            simulationBeginTime, Settings.TRANSACTION_SIZE.get(j), Settings.LEVELS.get(i),
                             0, ThreadPoolSize);
                    scheduler.run();*/

                    ExecutorService executorService = Executors.newFixedThreadPool(ThreadPoolSize + 5);
                    for (int temp = 0; temp < taskCount; temp++) {
                        //long curSimulationBeginTime = System.currentTimeMillis();
                        schedulerList[temp] = new QueryScheduler(connectionPool,
                                simulationBeginTime, Settings.TRANSACTION_SIZE.get(j), Settings.LEVELS.get(i),
                                temp * Settings.PERIOD, ThreadPoolSize);
                        int finalTemp = temp;
                        executorService.execute(schedulerList[finalTemp]);
                        /*scheduledFutures[temp] = scheduler.scheduleAtFixedRate(
                                () -> {
                                    schedulerList[finalTemp].run();
                                    latch.countDown();
                                }, 0, Settings.PERIOD * ThreadPoolSize, TimeUnit.MILLISECONDS);*/
                        Thread.sleep(Settings.PERIOD);
                    }

                    //System.out.println(System.currentTimeMillis() - simulationBeginTime);
                   /* try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
                        scheduledFuture.cancel(true);
                    }*/
                    //System.out.println(System.currentTimeMillis() - simulationBeginTime);


                    int counter = 0;
                    while(true) {
                        if (((ThreadPoolExecutor) executorService).getActiveCount() == 0) {
                            executorService.shutdown();
                            break;
                        }
                        //System.out.println("current waiting: " + ((ThreadPoolExecutor) executorService).getActiveCount());
                        Thread.sleep(100);
                        //System.out.println("not yet");
                    }

                    //System.out.println(System.currentTimeMillis() - simulationBeginTime);
                    //for test
                    /*List<String> test = new ArrayList<>();
                    test.add("SELECT ci.INFRASTRUCTURE_ID \n" +
                            "FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci \n" +
                            "WHERE sen.id=ci.SENSOR_ID AND sen.id='78dd9081_14a5_41eb_8632_14e45a6b1e57'");
                    executeSql(test, connectionPool, url, user, password);*/

                    long simulationEndTime = System.currentTimeMillis();
                    long totalTime = simulationEndTime - simulationBeginTime;
                    System.out.println("MPL:" + Settings.MPLS.get(k) +" Response time of the whole workload: " +
                            totalTime + "ms");

                    System.out.println("Transaction per second: " + ((Statistic.sqlSize / Settings.TRANSACTION_SIZE.get(j)) / (totalTime / 1000.0)) + "/s");
                    System.out.println("Total sqls: " + Statistic.sqlSize);
                    System.out.println("Total response time: " + Statistic.totalResponseTime + "ms");
                    System.out.println("Avg response time: " + ((Statistic.totalResponseTime * 1.0) / Statistic.sqlSize ) + "ms");
                    Thread.sleep(1000);

                    connectionPool.close();
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
            // get all sql during current time period

            List<String> currentQueries = new ArrayList<>();

            for (long i = counter; i < counter + Settings.PERIOD; i++) {
                List<String> sqls = sqlMap.getOrDefault(i, new ArrayList<>());
                for (String sql : sqls) {
                    currentQueries.add(sql);
                }
            }
            counter += Settings.PERIOD * ThreadPoolSize;
            int currentQueryCount = 0;
            Connection connection = null;
            PreparedStatement statement = null;
            long startTime = 0;
            try {
                //System.out.println("waiting at: " + (System.currentTimeMillis() - simulationBeginTime));
                connection = connectionPool.getConnection();
                startTime = System.currentTimeMillis();
                //System.out.println("start at: " + (startTime - simulationBeginTime));
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
                finishedTask--;
            } catch (SQLException e) {
                try {
                    if (connection != null) {
                        connection.rollback();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            } finally {
                try {
                    if (connection != null) {
                        connection.setAutoCommit(true);
                        connection.close();
                        long endTime = System.currentTimeMillis();

                        Statistic.totalResponseTime += (endTime - startTime);
                        //System.out.println("endTime: " + (endTime - simulationBeginTime));
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

