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
    // TODO: LOAD SQLS!
    private static HashMap<Long, List<String>> sqlMap = new HashMap<>();

    public static void postgresCleanUp() {
        try{
            SQLDataLoader.RunSQLByLine("Resources/schema/drop.sql");
            SQLDataLoader.RunSQLByLine("Resources/schema/create.sql");
            SQLDataLoader.RunSQLByLine(Settings.METADATA_DATASET_URL);
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        for (int i = 0; i < Settings.LEVELS.size(); i++) {
            for (int j = 0; j < Settings.TRANSACTION_SIZE.size(); j++) {
                for (int k = 0; k < Settings.MPLS.size(); k++) {
                    // create thread pool
                    ExecutorService executor = Executors.newFixedThreadPool(Settings.MPLS.get(k));
                    // create scheduled task
                    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                    long simulationBeginTime = System.currentTimeMillis();
                    // execute the task and get the ScheduledFuture instance
                    QueryScheduler queryScheduler = new QueryScheduler(executor,
                            simulationBeginTime, Settings.TRANSACTION_SIZE.get(j), Settings.LEVELS.get(i));
                    ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(
                            queryScheduler,0, Settings.PERIOD, TimeUnit.MILLISECONDS);
                    // wait for the task to complete
                    scheduledFuture.get();
                    // shutdown executor and scheduler
                    executor.shutdown();
                    scheduler.shutdown();
                    long simulationEndTime = System.currentTimeMillis();
                    System.out.println("Response time of the whole workload: " +
                            (long)(simulationEndTime - simulationBeginTime));
                    Thread.sleep(1000);
                }
            }
        }


    }



    static class QueryScheduler implements Runnable {
        private ExecutorService executor;
        private long simulationBeginTime;
        private int transactionSize;
        private int isolationLevel;

        public QueryScheduler(ExecutorService executor, long simulationBeginTime, int transactionSize, int isolationLevel) {
            this.executor = executor;
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
                List<String> sqls = sqlMap.getOrDefault(time - simulationBeginTime, new ArrayList<>());
                for (String sql : sqls) {
                    currentQueries.add(sql);
                }
            }

            if (currentQueries != null) {
                List<String> transaction = new ArrayList<>();
                int count = 0;
                for (String sql : currentQueries) {
                    transaction.add(sql);
                    count++;
                    if (count == transactionSize) {
                        executor.submit(new QueryTask(transaction, isolationLevel));
                        count = 0;
                    }
                }
                if (!transaction.isEmpty()) {
                    executor.submit(new QueryTask(transaction, isolationLevel));
                }
            }
        }
    }

    static class QueryTask implements Runnable {
        private List<String> transaction;
        private int isolationLevel;

        public QueryTask(List<String> transaction, int isolationLevel) {
            this.transaction = transaction;
            this.isolationLevel = isolationLevel;
        }

        @Override
        public void run() {
            //TODO: Monitor delay time
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = DriverManager.getConnection(url, user, password);
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(isolationLevel);


                // add sqls and commit them as one transaction
                stmt = conn.createStatement();
                for (String sql : transaction) {
                    stmt.executeUpdate(sql);
                }

                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    if (conn != null) {
                        conn.rollback();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}

