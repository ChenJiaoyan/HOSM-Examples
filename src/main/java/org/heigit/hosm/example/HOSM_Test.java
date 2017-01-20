package org.heigit.hosm.example;

/**
 * Created by Rtroilo on 1/19/17.
 */

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHNode;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHWay;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMNode;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMTag;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMUser;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMWay;

import org.heigit.bigspatialdata.osh.ignite.util.BoundingBox;
import org.heigit.hosm.example.Client.MyComputeJob;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class HOSM_Test {

    public static class MyJobOption implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Long> timestamps;
        private final Geometry bbox;
        private final int tagKey;

        public MyJobOption(final List<Long> timestamps, final Geometry bbox, final int tagKey) {
            this.timestamps = timestamps;
            this.bbox = bbox;
            this.tagKey = tagKey;
        }
    }

    public static class MyJobResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<Long, Long> timestampCount;

        public MyJobResult(final Map<Long, Long> tc) {
            this.timestampCount = tc;
        }

    }

    public static class MyJob extends ComputeJobAdapter {
        private static final long serialVersionUID = 1L;
        private final MyJobOption option;

        @IgniteInstanceResource
        private Ignite ignite;
        private final boolean localMode;

        public MyJob(MyJobOption option) {
            this.option = option;
            this.localMode = true;
        }

        public MyJob(final MyJobOption option, Ignite ignite, final boolean localMode) {
            this.option = option;
            this.localMode = localMode;
            this.ignite = ignite;
        }

        @Override
        public Object execute() throws IgniteException {
            IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");
            IgniteCache<AffinityKey<Long>, OSHWay> cacheWay = ignite.cache("osm_way");

            // search for all ways within a given bounding box;
            SqlQuery<AffinityKey<Long>, OSHWay> sqlWay = new SqlQuery<>(OSHWay.class, "BoundingBox && ?");
            sqlWay.setArgs(option.bbox);

            // wih this only the local cache is used
            sqlWay.setLocal(localMode);

            Map<Long, Long> result = new HashMap<>(option.timestamps.size());

            try (QueryCursor<Cache.Entry<AffinityKey<Long>, OSHWay>> cursor = cacheWay.query(sqlWay)) {
                for (Cache.Entry<AffinityKey<Long>, OSHWay> row : cursor) {
                    OSHWay oshWay = row.getValue();
                    // System.out.println(oshWay);
                    // get the valid way for each timestamp
                    Map<Long, OSMWay> timestampWayMap = oshWay.getByTimestamp(option.timestamps);
                    for (Map.Entry<Long, OSMWay> timestampWay : timestampWayMap.entrySet()) {
                        Long timestamp = timestampWay.getKey();
                        OSMWay way = timestampWay.getValue();
                        if (hasKey(way.getTags(), option.tagKey)) {

                            // TODO compute geometry of the way and check it
                            // again against the boundingbox
                            // Geometry g = getGeometry(way);

                            Long count = result.get(timestamp);
                            if (count == null) {
                                count = Long.valueOf(0);
                            }
                            count += 1;
                            result.put(timestamp, count);
                        }
                    }
                }
            }
            return new MyJobResult(result);
        }

        /*
         * tags is an index array of [key,value, key,value, ...] order by key!
         */
        private boolean hasKey(int[] tags, int key) {
            for (int i = 0; i < tags.length; i += 2) {
                if (tags[i] < key)
                    continue;
                if (tags[i] == key)
                    return true;
                return false;
            }
            return false;
        }

    }

    public static class MyTaskAdapter extends ComputeTaskAdapter<MyJobOption, MyJobResult> {

        private static final long serialVersionUID = 1L;

        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, MyJobOption arg)
                throws IgniteException {

            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

            MyJob myJob = new MyJob(arg);
            // for every node in the cluster!
            for (ClusterNode node : subgrid) {
                map.put(myJob, node);
            }
            return map;
        }

        @Override
        public MyJobResult reduce(List<ComputeJobResult> results) throws IgniteException {
            Map<Long, Long> reducedResult = new HashMap<>();
            for (ComputeJobResult computeJobResult : results) {
                MyJobResult rs = computeJobResult.<MyJobResult>getData();
                for (Map.Entry<Long, Long> entry : rs.timestampCount.entrySet()) {
                    Long timestamp = entry.getKey();
                    Long count = reducedResult.get(timestamp);
                    if (count == null) {
                        count = Long.valueOf(0);
                    }
                    count += entry.getValue();
                    reducedResult.put(timestamp, count);
                }
            }
            return new MyJobResult(reducedResult);
        }

    }

    public static void main(String[] args) throws IgniteCheckedException, ParseException, com.vividsolutions.jts.io.ParseException {
        System.out.println("############## test1 ##############");
        test1();

        //System.out.println("############## test2 ##############");
        //test2();
        //System.out.println("############## test3 ##############");
        //test3();
        //System.out.println("############## test4 ##############");
        //test4();
        //System.out.println("############## test5 ##############");
        //test5();
    }

    public static void test5() throws IgniteCheckedException {
        Ignition.setClientMode(true);

        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();

        try (Ignite ignite = Ignition.start(icfg)) {
            IgniteCache<Integer, OSHNode> cacheNodes = ignite.cache("osm_node");
            List<List<?>> rows = cacheNodes
                    .query(new SqlFieldsQuery("select _val from OSHNode")).getAll();

            if (rows == null || rows.isEmpty()) {
                System.err.println("Node not found!");
                return;
            }
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            for (int i = 0; i < rows.size(); i++) {
                OSHNode oshn = (OSHNode) rows.get(i).get(0);
                System.out.printf("lat: %d, lon: %d, version: %d \n", oshn.getLatitude(),oshn.getLongitude(),
                        oshn.getVersion(), formatter.format(new Date(oshn.getTimestamp())));
            }
        }
    }

    public static void test4() throws IgniteCheckedException {
        Ignition.setClientMode(true);

        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();

        try (Ignite ignite = Ignition.start(icfg)) {
            IgniteCache<Integer, OSHWay> cacheWays = ignite.cache("osm_way");
            List<List<?>> rows = cacheWays
                    .query(new SqlFieldsQuery("select _val from OSHWay")).getAll();

            if (rows == null || rows.isEmpty()) {
                System.err.println("Way not found!");
                return;
            }

            for (int i = 0; i < rows.size(); i++) {
                OSHWay osmw = (OSHWay) rows.get(i).get(0);
                BoundingBox bbox = osmw.getBBoxInternal();
                System.out.printf("%d, %d, %d, %d \n", bbox.getX1(),bbox.getY1(),bbox.getX2(),bbox.getY2());
                Geometry g = osmw.getBoundingBox();
                System.out.printf("%s \n", Arrays.toString(g.getCoordinates()));
            }
        }
    }

    public static void test3() throws IgniteCheckedException {
        Ignition.setClientMode(true);

        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();

        try (Ignite ignite = Ignition.start(icfg)) {
            //IgniteCache<AffinityKey<Long>, OSMNode> cacheNode = ignite.cache("osm_node");
            IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
            List<List<?>> rows = cacheTags
                    .query(new SqlFieldsQuery("select key,number,numbers,values from OSMTag")).getAll();

            if (rows == null || rows.isEmpty()) {
                System.err.println("Node not found!");
                return;
            }

            for (int i = 0; i < rows.size(); i++) {
                System.out.printf("%s, %s, %s \n", rows.get(i).get(0), rows.get(i).get(1),
                        Arrays.toString((int []) rows.get(i).get(2)));
                System.out.printf("%s \n", Arrays.toString((Object[]) rows.get(i).get(3)));
                if(i==20){
                    break;
                }
            }
        }
    }


    public static void test2() throws IgniteCheckedException {
        Ignition.setClientMode(true);

        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();

        //String tag = "building";
        try (Ignite ignite = Ignition.start(icfg)) {
            IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
            List<List<?>> rows = cacheTags
                    .query(new SqlFieldsQuery("select _val from OSMTag")).getAll();

            if (rows == null || rows.isEmpty()) {
                System.err.println("Tags with key building not found!");
                return;
            }
            ArrayList<String> keys = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                OSMTag osmt = (OSMTag) rows.get(i).get(0);
                keys.add(osmt.getKey());
                System.out.printf("%s,  %s, %s \n", osmt.getKey(), Arrays.toString(osmt.getValues()),
                        Arrays.toString(osmt.getNumbers()));
            }
            System.out.println(keys);
        }

    }

    public static void test1()
            throws IgniteCheckedException, ParseException, com.vividsolutions.jts.io.ParseException {
        Ignition.setClientMode(true);

        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();

        //String tag = "building";
        //String tag = "toilets";
        //String tag = "park";
        //String tag = "traffic_sign";
        //String tag = "atm";
        //String tag = "route";
        String tag = "highway";

        try (Ignite ignite = Ignition.start(icfg)) {
            IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
            List<List<?>> rows = cacheTags
                    .query(new SqlFieldsQuery("select _key from OSMTag where key = ?").setArgs(tag)).getAll();
            if (rows == null || rows.isEmpty()) {
                System.err.println("Tags with key building not found!");
                return;
            }

            int buildingsKey = ((Integer) rows.get(0).get(0)).intValue();

            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            List<Long> timestamps = Arrays.asList(formatter.parse("20170101").getTime(),
                    formatter.parse("20160101").getTime(), formatter.parse("20150101").getTime(),
                    formatter.parse("20140101").getTime(), formatter.parse("20130101").getTime(),
                    formatter.parse("20120101").getTime(), formatter.parse("20120102").getTime());

            WKTReader r = new WKTReader();
            // http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
//          Geometry bbox = r.read(
//                    "POLYGON((12.357822060585022 45.42796074630555,12.358822524547577 45.42796074630555,12.358822524547577 45.427420498069445,12.357822060585022 45.427420498069445,12.357822060585022 45.42796074630555))");
            //Geometry bbox = r.read(
             //       "POLYGON((12.310524 45.445372,12.347603 45.444649,12.349663 45.427303,12.304344 45.428026,12.310524 45.445372))");
            Geometry bbox = r.read(
                   " POLYGON((12.297821044921875 45.45174687098183,12.371635437011719 45.45174687098183,12.371635437011719 45.4187415580181,12.297821044921875 45.4187415580181,12.297821044921875 45.45174687098183))");

            MyJobOption option = new MyJobOption(timestamps, bbox, buildingsKey);

            IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());

            // Do it on the Server
            MyJobResult result;

            boolean onServer = false;

            if (onServer) {

                result = compute.execute(MyTaskAdapter.class, option);
            } else {
                MyJob myJob = new MyJob(option, ignite, false);
                result = (MyJobResult) myJob.execute();
            }

            for (Map.Entry<Long, Long> timeCount : result.timestampCount.entrySet()) {
                System.out.printf("%s : %d\n", formatter.format(new Date(timeCount.getKey())), timeCount.getValue());
            }

        }

    }

}
