package com.wxmimperio.flink;

import com.datastax.driver.core.*;
import com.wxmimperio.flink.utils.CSVHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wxmimperio on 2017/7/22.
 */
public class InsertData {
    private static final Logger LOG = LoggerFactory.getLogger(InsertData.class);

    private static Cluster cluster;
    private static Session session;


    private static final ThreadLocal<SimpleDateFormat> ascFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static void init() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 20);
        Cluster.Builder builder = Cluster.builder();
        for (String connect : "".split(",", -1)) {
            builder.addContactPoints(connect);
        }
        cluster = builder.withPoolingOptions(poolingOptions).build();
        session = cluster.connect();
        LOG.info("Cassandra Cluster build!");
    }

    public static boolean executeStatement(BatchStatement batchStatement, String topic) {
        try {
            if (session != null) {
                session.execute(batchStatement);
            }
        } catch (Exception e) {
            LOG.error("Execute Batch Statement error:" + e + " topic:" + topic);
            return false;
        }
        return true;
    }

    public static PreparedStatement prepareBatch(String sql) {
        PreparedStatement prepareBatch = null;
        try {
            if (session != null) {
                prepareBatch = session.prepare(sql);
            }
        } catch (Exception e) {
            LOG.error("Prepare Statement error:" + e);
        }
        return prepareBatch;
    }

    public static String minuteHourChange(Calendar cal, int splitMinute) throws Exception {
        //所有时间统一为cal的second、millisecond（为了计算差值，精确到millisecond）
        int second = cal.get(Calendar.SECOND);
        int millisecond = cal.get(Calendar.MILLISECOND);

        //baseTime，即以当前小时，零分为基准，根据splitMinute计算时间起始点
        long nowTimeStamp = cal.getTimeInMillis();
        int baseMinute = (cal.get(Calendar.MINUTE) / splitMinute) * splitMinute;
        int baseHour = cal.get(Calendar.HOUR_OF_DAY);
        int baseDay = cal.get(Calendar.DAY_OF_MONTH);
        int baseMonth = cal.get(Calendar.MONTH) + 1;
        int baseYear = cal.get(Calendar.YEAR);

        if (baseMinute > 60) {
            baseHour += 1;
        }

        String baseTime = baseYear + "-" + addZero(baseMonth, 2) + "-" + addZero(baseDay, 2) +
                " " + addZero(baseHour, 2) + ":" + addZero((cal.get(Calendar.MINUTE) / splitMinute) * splitMinute, 2)
                + ":" + addZero(second, 2) + ":" + millisecond;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
        //System.out.println("base time = " + baseTime);
        Date baseDate = sdf.parse(baseTime);
        Calendar baseCal = new GregorianCalendar();
        baseCal.setTime(baseDate);

        //delayTime为以baseTime为基准，计算splitMinute为间隔的下一次时间点
        int hour = (baseCal.get(Calendar.HOUR_OF_DAY) + ((baseCal.get(Calendar.MINUTE) + splitMinute) / 60));
        int minute = (baseCal.get(Calendar.MINUTE) + splitMinute) % 60;
        int day = baseCal.get(Calendar.DAY_OF_MONTH) + hour / 24;
        int month = baseCal.get(Calendar.MONTH) + 1;
        int year = baseCal.get(Calendar.YEAR);

        if (day > baseCal.getActualMaximum(Calendar.DAY_OF_MONTH)) {
            day = day % baseCal.getActualMaximum(Calendar.DAY_OF_MONTH);
            month = (baseCal.get(Calendar.MONTH) + 1) + (day % baseCal.getActualMaximum(Calendar.DAY_OF_MONTH));
            if (month > (baseCal.getActualMaximum(Calendar.MONTH) + 1)) {
                month = 1;
                year = baseCal.get(Calendar.YEAR) + (month % baseCal.getActualMaximum(Calendar.MONTH));
            }
        }

        if (hour >= 24) {
            hour = 0;
        }

        String delayTime = addZero(year, 2) + "-" + addZero(month, 2) + "-" + addZero(day, 2) +
                " " + addZero(hour, 2) + ":" + addZero(minute, 2) + ":" + addZero(second, 2) + ":" + millisecond;
        //System.out.println("delay time = " + delayTime);
        Date delayDate = sdf.parse(delayTime);
        Calendar delayCal = new GregorianCalendar();
        delayCal.setTime(delayDate);

        long delayTimeStamp = delayCal.getTimeInMillis();

        //最终归类时间：下一次间隔时间 - 当前时间，如果大于splitMinute则进入下一次时间间隔，否则留在当前时间
        String doneTime;
        if ((delayTimeStamp - nowTimeStamp) >= splitMinute * 60 * 1000) {
            doneTime = baseTime;
        } else {
            doneTime = delayTime;
        }
        return doneTime;
    }

    public static String addZero(int num, int len) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(num);
        while (stringBuilder.length() < len) {
            stringBuilder.insert(0, "0");
        }
        return stringBuilder.toString();
    }

    public static List<Object> prepareData(List dataList, int index, int splitMinute) {
        List<Object> objects = new ArrayList<>();
        objects = dataList;

        try {

            //event_time == cassandra_time
            Calendar cal = Calendar.getInstance();
            Date cassandraDate = new Date();
            try {
                cassandraDate = ascFormat.get().parse(dataList.get(6).toString());
            } catch (Exception e) {
                LOG.error("event_time parse error. event_time=" + " gr=", e);
            }
            cal.setTime(cassandraDate);
            String time = minuteHourChange(cal, splitMinute);

            // Insert cassandra_time and message_id
            objects.add(0, time.substring(0, 16));
            objects.add(1, String.valueOf(index));
        } catch (Exception e) {
            LOG.error("" + e);
        }
        return objects;
    }

    public static void main(String[] args) {
        String sql = "INSERT INTO rtc.sxcq_character_glog (cassandra_time,message_id," +
                "area_id," +
                "career_id," +
                "channel_id," +
                "character_id," +
                "character_name," +
                "device_id," +
                "event_time," +
                "game_id," +
                "gender," +
                "group_id," +
                "ip," +
                "mid," +
                "platform" +
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        init();
        PreparedStatement prepareBatch = prepareBatch(sql);
        BatchStatement batchStatement = new BatchStatement();


        System.out.println(sql);


        /*cassandra_time	varchar
        message_id	varchar
        area_id	integer
        career_id	integer
        channel_id	varchar
        character_id	varchar
        character_name	varchar
        device_id	varchar
        event_time	varchar
        game_id	integer
        gender	integer
        group_id	integer
        ip	varchar
        mid	varchar
        platform	integer*/

        //[area_id, career_id, channel_id, character_id, character_name,
        // device_id, event_time, game_id, gender, group_id, ip, mid, platform]

        String filePath = "E:\\data.csv";

        //[event_time 0, game_id 1, area_id 2, group_id 3, mid 4, character_id 5, character_name 6,
        // channel_id 7, device_id 8, ip 9, platform 10, career_id 11, gender 12]

        List<String[]> csvFile_new = CSVHelper.readCSVFile(filePath, ',');

        List<List> new_str = new ArrayList<>();

        for (String[] str : csvFile_new) {
            //System.out.println(Arrays.asList(str));

            try {
                List list = new ArrayList();
                list.add(Integer.parseInt(str[2]));
                list.add(Integer.parseInt(str[11]));
                list.add(str[7]);
                list.add(str[5]);
                list.add(str[6]);
                list.add(str[8]);
                list.add(str[0]);
                list.add(Integer.parseInt(str[1]));
                list.add(Integer.parseInt(str[12]));
                list.add(str[3]);
                list.add(str[9]);
                list.add(str[4]);
                list.add(Integer.parseInt(str[10]));

                new_str.add(list);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("=========================");
        System.out.println("=========================");

        for (int i = 0; i < new_str.size(); i++) {
            List<Object> objects = prepareData(new_str.get(i), i, 1);

            System.out.println(objects);

            batchStatement.add(prepareBatch.bind(objects));
        }
        //insertData.executeStatement(batchStatement, "sxcq");
    }
}
