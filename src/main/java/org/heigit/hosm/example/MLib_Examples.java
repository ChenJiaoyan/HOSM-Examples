package org.heigit.hosm.example;

import org.apache.avro.generic.GenericData;
import org.apache.ignite.IgniteCheckedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by John on 1/21/17.
 */
public class MLib_Examples {
    public static void main(String args []) throws IgniteCheckedException, ParseException, com.vividsolutions.jts.io.ParseException {
        String tagKey = "building";
        String tagValue = "hut";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String start_time_str = "20120101";
        String end_time_str = "20170101";
        int step_month = 1;

        ArrayList<Long> times = new ArrayList<>();
        Calendar it = Calendar.getInstance();
        it.setTime(formatter.parse(start_time_str));
        Calendar end_it = Calendar.getInstance();
        end_it.setTime(formatter.parse(end_time_str));
        while(it.compareTo(end_it)<=0){
            times.add(it.getTimeInMillis());
            it.add(Calendar.MONTH,step_month);
        }

        String polygon_str = "POLYGON((12.297821044921875 45.45174687098183,12.371635437011719 45.45174687098183,12.371635437011719 45.4187415580181,12.297821044921875 45.4187415580181,12.297821044921875 45.45174687098183))";

        HOSMClient client = new HOSMClient();
        Map<Long,Long> counts = client.spatial_temporal_count(tagKey,tagValue, times ,polygon_str);

        for(int i=0; i<times.size();i++){
            Long t = times.get(i);
            System.out.printf("%s: %d \n",formatter.format(new Date(t)),counts.get(t));
        }
    }
}
