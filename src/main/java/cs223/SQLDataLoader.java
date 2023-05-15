package cs223;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SQLDataLoader {

    public static void LoadSQL(String filePath, HashMap<Long, List<String>> sqlMap) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("INSERT INTO")) {
                    String timestampString = line.substring(line.indexOf(", '") + 3, line.indexOf("', '"));
                    long timestamp = java.sql.Timestamp.valueOf(timestampString).getTime();
                    timestamp = (timestamp - java.sql.Timestamp.valueOf(Settings.OBSERVATION_START_DATE).getTime()) / Settings.TIME_SCALE_RATIO;
                    if (!sqlMap.containsKey(timestamp)) {
                        sqlMap.put(timestamp, new ArrayList<>());
                    }
                    List<String> sqls = sqlMap.get(timestamp);
                    sqls.add(line);
                }
                counter++;
                if (counter > 10000) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Long timestamp : sqlMap.keySet()) {
            System.out.println(timestamp + ": " + sqlMap.get(timestamp));
        }
    }

    public static void LoadSQL(String filePath, List<String> sqlList) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("--") || line.startsWith("//") || line.startsWith("/*")) {
                    continue;
                }
                sb.append(line.trim());

                if (line.trim().endsWith(";")) {
                    sqlList.add(sb.toString());
                    sb.setLength(0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

