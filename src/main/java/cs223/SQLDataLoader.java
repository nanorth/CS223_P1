package cs223;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLDataLoader {

    public static void LoadSQL(String filePath, HashMap<Long, List<String>> sqlMap) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String regex = "'(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})'";

            Pattern pattern = Pattern.compile(regex);

            String line;
            int counter = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("INSERT INTO")) {
                    Matcher matcher = pattern.matcher(line);
                    String timestampString = "";
                    if (matcher.find()) {
                        timestampString = matcher.group(1);
                    }
                    long timestamp = java.sql.Timestamp.valueOf(timestampString).getTime();
                    timestamp = (timestamp - java.sql.Timestamp.valueOf(Settings.OBSERVATION_START_DATE).getTime()) / Settings.TIME_SCALE_RATIO;
                    if (timestamp < Settings.SIMULATION_LENGTH) {
                        if (!sqlMap.containsKey(timestamp)) {
                            sqlMap.put(timestamp, new ArrayList<>());
                        }
                        List<String> sqls = sqlMap.get(timestamp);
                        sqls.add(line);
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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

    public static void LoadQueries(String filePath, HashMap<Long, List<String>> sqlMap) {
        File file = new File(filePath);

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            String sqlString = "";
            int counter = 0;
            int total = 0;
            while ((line = br.readLine()) != null) {
                sqlString += line + " ";
                if (line.charAt(0) == '"') {
                    int lastQuote = sqlString.lastIndexOf('"');
                    int firstQuote = sqlString.lastIndexOf('"', lastQuote - 1);
                    String timeString = sqlString.substring(0, firstQuote - 1).replace("T", " ").replace("Z", "");
                    long timestamp = java.sql.Timestamp.valueOf(timeString).getTime();
                    timestamp = (timestamp - java.sql.Timestamp.valueOf(Settings.OBSERVATION_START_DATE).getTime()) / Settings.TIME_SCALE_RATIO;
                    String sql = sqlString.substring(firstQuote + 2, lastQuote - 1);
                    if (sqlMap.containsKey(timestamp)) {
                        sqlMap.get(timestamp).add(sql);
                    } else {
                        List<String> sqlList = new ArrayList<>();
                        sqlList.add(sql);
                        sqlMap.put(timestamp, sqlList);
                    }
                    sqlString = "";
                    total++;
                    if (timestamp > Settings.SIMULATION_LENGTH) {
                        if (counter++ > 10)
                            break;
                    }


                }
            }
            System.out.println(total);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }





    // TODO: Another function to load queries for txt file to sqlMap
}

