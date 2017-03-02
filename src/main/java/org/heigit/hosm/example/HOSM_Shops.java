package org.heigit.hosm.example;

import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.ignite.IgniteCheckedException;

/**
 * Created by John on 3/2/17.
 */

public class HOSM_Shops {
    public static void main(String args[]) throws ParseException, IgniteCheckedException, com.vividsolutions.jts.io.ParseException {
        String tagKey = "Shop";

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String start_time_str = "20100101";
        String end_time_str = "20170301";
        int step_month = 1;

        ArrayList<Long> times = new ArrayList<>();
        Calendar it = Calendar.getInstance();
        it.setTime(formatter.parse(start_time_str));
        Calendar end_it = Calendar.getInstance();
        end_it.setTime(formatter.parse(end_time_str));
        while (it.compareTo(end_it) <= 0) {
            times.add(it.getTimeInMillis());
            it.add(Calendar.MONTH, step_month);
        }

        //String polygon_str = "POLYGON((12.297821044921875 45.45174687098183,12.371635437011719 45.45174687098183,12.371635437011719 45.4187415580181,12.297821044921875 45.4187415580181,12.297821044921875 45.45174687098183))";
        String polygon_str = "POLYGON((119.96177673339844 30.38720294760581,120.35041809082031 30.38720294760581," +
                "120.35041809082031 30.104259174773546,119.96177673339844 30.104259174773546,119.96177673339844 30.38720294760581))";

        System.out.println("#### count the " + tagKey + " #####");
        HOSMClient client = new HOSMClient();
        Map<Long, Long> counts = client.spatial_temporal_count(tagKey, times, polygon_str);

        for (int i = 0; i < times.size(); i++) {
            Long t = times.get(i);
            long count = (counts.get(t) == null) ? 0 : counts.get(t);
            System.out.printf("%s: %d \n", formatter.format(new Date(t)), count);
        }
    }

    public static void read_object_time(String key, String polygon_str, Long time) {
        HOSMClient client = new HOSMClient();
    }

}
