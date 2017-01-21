package org.heigit.hosm.example;

import org.apache.commons.lang.ArrayUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;


import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by John on 1/21/17.
 */
public class MLib_Examples {

    public static void main(String args[]) throws URISyntaxException, ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        String tagKey = "building";
        String [] tagValues = {"hut","roof","industrial","residential"};
        List<String> items = new ArrayList<String>();
        items.add(tagKey);
        for(String v:tagValues){
            items.add(v);
        }

        List<Double []> osm_count = read_HOSM(tagKey,tagValues);

        SparkConf sparkConf = null;
        if (args.length == 0) {
            sparkConf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local");
        } else {
            sparkConf = new SparkConf().setAppName("SVM Classifier Example");
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        /**
         * Example 1: calculate the Pearson correlation coefficient between two time-series
         */
        String result1 = "";
        for(int i=0;i<items.size()-1;i++){
            for(int j=i+1;j<items.size();j++){
                JavaDoubleRDD ri = jsc.parallelizeDoubles(Arrays.asList(osm_count.get(i)));
                JavaDoubleRDD rj = jsc.parallelizeDoubles(Arrays.asList(osm_count.get(j)));
                Double correlation = Statistics.corr(ri.srdd(), rj.srdd(), "pearson");
                result1 += String.format("(%s, %s) corr: %f \n", items.get(i),items.get(j),correlation);
            }
        }
        System.out.println(result1);

        /**
         * Example 2:  Compute column summary statistics.
         */
        List<Vector> osm_count_v = new ArrayList<Vector>();
        for(Double [] item: osm_count){
            osm_count_v.add(Vectors.dense(ArrayUtils.toPrimitive(item)));
        }
        JavaRDD<Vector> osm_count_mat = jsc.parallelize(osm_count_v);
        osm_count_mat = jsc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(3.0, 30.0, 300.0)
                )
        );
        MultivariateStatisticalSummary summary = Statistics.colStats(osm_count_mat.rdd());
        System.out.println(summary.mean());
        System.out.println(summary.variance());
        System.out.println(summary.numNonzeros());
    }

    public static List<Double []> read_HOSM(String tagKey, String [] tagValues)
            throws IgniteCheckedException, ParseException, com.vividsolutions.jts.io.ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String start_time_str = "20121201";
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
        List<Double []> osm_counts = new ArrayList<Double[]>();

        System.out.println("#### count the buildings #####");
        Map<Long,Long> counts = client.spatial_temporal_count(tagKey, times ,polygon_str);
        Double [] d = new Double[times.size()];
        for (int i = 0; i < times.size(); i++) {
            Long t = times.get(i);
            long count = (counts.get(t) == null) ? 0 : counts.get(t);
            System.out.printf("%s: %d \n", formatter.format(new Date(t)), count);
            d[i] = (double)count;
        }
        osm_counts.add(d);

        for(int k=0;k<tagValues.length;k++) {
            String tagValue = tagValues[k];
            System.out.println("#### count the buildings of "+tagValue+" #####");
            counts = client.spatial_temporal_count(tagKey, tagValue, times, polygon_str);
            d = new Double[times.size()];
            for (int i = 0; i < times.size(); i++) {
                Long t = times.get(i);
                long count = (counts.get(t) == null) ? 0 : counts.get(t);
                System.out.printf("%s: %d \n", formatter.format(new Date(t)), count);
                d[i] = (double)count;
            }
            osm_counts.add(d);
        }

        return osm_counts;
    }
}
