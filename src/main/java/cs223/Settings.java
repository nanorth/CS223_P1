package cs223;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Settings {

    public static List<Integer> MPLS = new ArrayList<Integer>(Arrays.asList(1, 3, 5, 10, 20, 40, 100));
    //public static List<Integer> MPLS = new ArrayList<Integer>(Arrays.asList(40));

    //public static List<Integer> TRANSACTION_SIZE = new ArrayList<Integer>(Arrays.asList(1, 5, 10, 40));
    public static List<Integer> TRANSACTION_SIZE = new ArrayList<Integer>(Arrays.asList(20));

    public static List<Integer> LEVELS = new ArrayList<>(Arrays.asList(
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE));

    public static int PERIOD = 1; // simulation time step in millisecond

    public static String OBSERVATION_START_DATE = "2017-11-08 00:00:00"; // the earliest timestamp of observation insert info query

    public static int TIME_SCALE_RATIO = 1440 * 10; // use this to scale 20 days to 2 minutes

    public static long SIMULATION_LENGTH = 3000; // in millisecond

    public static String SEMANTIC_DATASET_URL = "Resources/data/low_concurrency/semantic_observation_low_concurrency.sql";

    public static String OBSERVATION_DATASET_URL = "Resources/data/low_concurrency/observation_low_concurrency.sql";

    public static String METADATA_DATASET_URL = "Resources/data/low_concurrency/metadata.sql";

    public static String QUERY_DATA_URL = "Resources/queries/low_concurrency/queries.txt";

    public static boolean HIGH_CONCURRENCY = false;

    public static void switch_to_high_concurrency() {
        SEMANTIC_DATASET_URL = "Resources/data/high_concurrency/semantic_observation_high_concurrency.sql";
        OBSERVATION_DATASET_URL = "Resources/data/high_concurrency/observation_high_concurrency.sql";
        METADATA_DATASET_URL = "Resources/data/high_concurrency/metadata.sql";
        QUERY_DATA_URL = "Resources/queries/high_concurrency/queries.txt";
    }
}