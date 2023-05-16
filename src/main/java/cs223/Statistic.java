package cs223;

public class Statistic {
    public static int sqlSize = 0;
    public static long totalResponseTime = 0;

    public static void clear() {
        sqlSize = 0;
        totalResponseTime = 0;
    }
}
