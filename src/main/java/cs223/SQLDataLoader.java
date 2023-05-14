package cs223;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SQLDataLoader {

    public static void LoadSQLByLine(String filePath, HashMap<Long, List<String>> sqlMap) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("INSERT INTO")) {
                    String timestampString = line.substring(line.indexOf(", '") + 3, line.indexOf("', '"));
                    long timestamp = java.sql.Timestamp.valueOf(timestampString).getTime();
                    List<String> sqls = sqlMap.getOrDefault(timestamp, new ArrayList<>());
                    sqls.add(line);
                }
                counter++;
                if (counter > 1000) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Long timestamp : sqlMap.keySet()) {
            System.out.println(timestamp + ": " + sqlMap.get(timestamp));
        }
    }

    public static void RunSQLByLine(String filePath, HashMap<Long, List<String>> sqlMap) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("INSERT INTO")) {
                    String timestampString = line.substring(line.indexOf(", '") + 3, line.indexOf("', '"));
                    long timestamp = java.sql.Timestamp.valueOf(timestampString).getTime();
                    List<String> sqls = sqlMap.getOrDefault(timestamp, new ArrayList<>());
                    sqls.add(line);
                }
                counter++;
                if (counter > 1000) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Long timestamp : sqlMap.keySet()) {
            System.out.println(timestamp + ": " + sqlMap.get(timestamp));
        }
    }
}

