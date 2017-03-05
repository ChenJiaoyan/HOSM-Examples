
package org.heigit.hosm.example;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHNode;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHWay;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMNode;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMTag;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMWay;

import javax.cache.Cache;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Rtroilo on 1/19/17.
 * Latest update: March 3, 2017, by Jiaoyan
 */

public class HOSM_Select {


    public static class JobOption implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Long> timestamps;
        private final Geometry bbox;
        private final ArrayList<int[]> tag_ids;

        public JobOption(final List<Long> timestamps, final Geometry bbox, final ArrayList<int[]> tag_ids) {
            this.timestamps = timestamps;
            this.bbox = bbox;
            this.tag_ids = tag_ids;
        }

    }

    public static class JobResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<Long, ArrayList<String>> timestampSelect;

        public JobResult(final Map<Long, ArrayList<String>> tc) {
            this.timestampSelect = tc;
        }
    }

    public static class SelectJob extends ComputeJobAdapter {
        private static final long serialVersionUID = 1L;
        private final JobOption option;

        @IgniteInstanceResource
        private Ignite ignite;
        private final boolean localMode;

        private String[] object_types;

        public SelectJob(JobOption option) {
            this.option = option;
            this.localMode = true;
            this.object_types = new String[]{"way", "node"};
        }

        public SelectJob(final JobOption option, Ignite ignite, final boolean localMode) {
            this.option = option;
            this.localMode = localMode;
            this.ignite = ignite;
            this.object_types = new String[]{"way", "node"};
        }

        public SelectJob(final JobOption option, Ignite ignite, final boolean localMode, String[] object_types) {
            this.option = option;
            this.localMode = localMode;
            this.ignite = ignite;
            this.object_types = object_types;
        }

        @Override
        public Object execute() throws IgniteException {
            Map<Long, ArrayList<String>> result = new HashMap<>(option.timestamps.size());
            for (int i = 0; i < this.object_types.length; i++) {
                switch (object_types[i]) {
                    case "way":
                        result = printWay(result);
                        break;
                    case "node":
                        result = printNode(result);
                        break;
                    default:
                        break;
                }
            }
            return new JobResult(result);
        }

        private Map<Long, ArrayList<String>> printWay(Map<Long, ArrayList<String>> result) {
            IgniteCache<AffinityKey<Long>, OSHWay> cacheWay = ignite.cache("osm_way");
            SqlQuery<AffinityKey<Long>, OSHWay> sqlWay = new SqlQuery<>(OSHWay.class, "BoundingBox && ?");
            sqlWay.setArgs(option.bbox);
            sqlWay.setLocal(localMode);

            try (QueryCursor<Cache.Entry<AffinityKey<Long>, OSHWay>> cursor = cacheWay.query(sqlWay)) {
                for (Cache.Entry<AffinityKey<Long>, OSHWay> row : cursor) {
                    OSHWay oshWay = row.getValue();
                    Map<Long, OSMWay> timestampWayMap = oshWay.getByTimestamp(option.timestamps);
                    for (Map.Entry<Long, OSMWay> timestampWay : timestampWayMap.entrySet()) {
                        Long timestamp = timestampWay.getKey();
                        OSMWay way = timestampWay.getValue();
                        int[] way_tags = way.getTags();
                        if (hasKeyValue(way_tags, option.tag_ids)) {
                            String tags_s = tags2string(way_tags);
                            String node_id = way.toString().split(" ")[1].split(":")[1];
                            String s = String.format("way,%s,,,%s", node_id, tags_s);
                            if (result.containsKey(timestamp)) {
                                ArrayList<String> r = result.get(timestamp);
                                r.add(s);
                                result.put(timestamp, r);
                            } else {
                                ArrayList<String> r = new ArrayList<>();
                                r.add(s);
                                result.put(timestamp, r);
                            }

                        }
                    }
                }
            }
            return result;
        }

        private Map<Long, ArrayList<String>> printNode(Map<Long, ArrayList<String>> result) {
            IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");
            SqlQuery<AffinityKey<Long>, OSHNode> sqlNode = new SqlQuery<>(OSHNode.class, "BoundingBox && ?");
            sqlNode.setArgs(option.bbox);
            sqlNode.setLocal(localMode);

            double GEOM_PRECISION = .000000001;
            try (QueryCursor<Cache.Entry<AffinityKey<Long>, OSHNode>> cursor = cacheNode.query(sqlNode)) {
                for (Cache.Entry<AffinityKey<Long>, OSHNode> row : cursor) {
                    OSHNode oshNode = row.getValue();
                    Map<Long, OSMNode> timestampNodeMap = oshNode.getByTimestamp(option.timestamps);

                    for (Map.Entry<Long, OSMNode> timestampNode : timestampNodeMap.entrySet()) {
                        Long timestamp = timestampNode.getKey();
                        OSMNode node = timestampNode.getValue();
                        if (hasKeyValue(node.getTags(), option.tag_ids)) {
                            double lat = node.getLatitude() * GEOM_PRECISION;
                            double lon = node.getLongitude() * GEOM_PRECISION;
                            int[] tags = node.getTags();
                            String tags_s = tags2string(tags);
                            String node_id = node.toString().split(" ")[1].split(":")[1];
                            String s = String.format("node,%s,%f,%f,%s", node_id, lat, lon, tags_s);
                            if (result.containsKey(timestamp)) {
                                ArrayList<String> r = result.get(timestamp);
                                r.add(s);
                                result.put(timestamp, r);
                            } else {
                                ArrayList<String> r = new ArrayList<>();
                                r.add(s);
                                result.put(timestamp, r);
                            }

                        }
                    }

                }
            }
            return result;
        }


        private String tags2string(int[] tags) {
            if (tags == null || tags.length == 0) {
                return "empty";
            } else if (tags.length % 2 != 0) {
                return "wrong";
            } else {
                IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
                String s = "";
                for (int i = 0; i < tags.length; i = i + 2) {
                    int key_id = tags[i];
                    int value_id = tags[i + 1];
                    OSMTag tag = cacheTags.get(key_id);
                    s = s + String.format("%s:%s", tag.getKey(), tag.getValue(value_id)) + ";";
                }
                return s;
            }
        }

        /*
        * tags is an index array of [key,value, key,value, ...] order by key!
        */
        private boolean hasKeyValue(int[] tags, ArrayList<int[]> tag_ids) {
            for (int[] tag_id : tag_ids) {
                for (int i = 0; i < tags.length; i += 2) {
                    if (tags[i] == tag_id[0]) {
                        if (tag_id[1] == -1) {
                            return true;
                        } else {
                            if (tags[i + 1] == tag_id[1]) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    }
                    return false;
                }
            }
            return false;
        }
    }


    public Map<Long, ArrayList<String>> spatial_temporal_select(String[] tags, ArrayList<Long> times_arr, String polygon_str,
                                                                String[] obj_types) throws ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        Ignition.setClientMode(true);
        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();
        try (Ignite ignite = Ignition.start(icfg)) {
            ArrayList<int[]> tag_ids = get_tag_id(ignite, tags);
            if (tag_ids != null && tag_ids.size() > 0) {
                List<Long> timestamps = times_arr;
                WKTReader r = new WKTReader();
                Geometry bbox = r.read(polygon_str);
                JobOption option = new JobOption(timestamps, bbox, tag_ids);
                IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());
                SelectJob myJob = new SelectJob(option, ignite, false, obj_types);
                JobResult result = (JobResult) myJob.execute();
                return result.timestampSelect;
            } else {
                return null;
            }
        }
    }


    private ArrayList<int[]> get_tag_id(Ignite ignite, String[] tags) {
        ArrayList<int[]> tag_ids = new ArrayList<>();
        IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
        for (String tag : tags) {
            String key = tag.split(":")[0];
            List<List<?>> rows = cacheTags
                    .query(new SqlFieldsQuery("select _key,values from OSMTag where key = ?").setArgs(key)).getAll();
            if (rows == null || rows.isEmpty()) {
                continue;
            }
            int key_id = ((Integer) rows.get(0).get(0)).intValue();
            Object[] values = (Object[]) rows.get(0).get(1);

            if (tag.split(":").length > 1) {
                String value = tag.split(":")[1];
                String[] value_split = tag.split(":")[1].split(",");
                Arrays.sort(value_split);
                for (int i = 0; i < values.length; i++) {
                    String value_i = (String) values[i];
                    if (Arrays.binarySearch(value_split, value_i) >= 0) {
                        tag_ids.add(new int[]{key_id, i});
                    }
                }
            } else {
                tag_ids.add(new int[]{key_id, -1});
            }
        }
        return tag_ids;
    }

    private void save2file(File f, ArrayList<Long> times, Map<Long, ArrayList<String>> results) throws IOException {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        for (int i = 0; i < times.size(); i++) {
            long t = times.get(i);
            ArrayList<String> rs = results.get(t);
            Date resultdate = new Date(t);
            String ts = formatter.format(resultdate);
            String fname = f.getAbsolutePath() + "/" + ts + ".csv";
            File tf = new File(fname);
            tf.createNewFile();
            FileWriter fileWriter = new FileWriter(tf);
            for (String r : rs) {
                fileWriter.write(r + "\n");
            }
            fileWriter.close();
            System.out.printf("");
        }
    }

    public static void main(String args[]) throws ParseException, IgniteCheckedException, com.vividsolutions.jts.io.ParseException, IOException {
        String dir = args[0];
        File f = new File(dir);
        if (f.exists()) {
            System.out.printf("the output exist \n");
            System.exit(0);
        } else {
            f.mkdir();
        }

        String[] tags = new String[]{"shop", "building:commercial"};

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

        String polygon_str = "POLYGON((117.762451171875 32.12154573409534,122.530517578125 32.12154573409534," +
                "122.530517578125 28.199742006199717,117.762451171875 28.199742006199717,117.762451171875 32.12154573409534))";
        String[] obj_types = new String[]{"node", "way"};

        System.out.println("#### select the objects with tags: '" + Arrays.toString(tags) + "' #####");
        HOSM_Select client = new HOSM_Select();
        Map<Long, ArrayList<String>> results = client.spatial_temporal_select(tags, times, polygon_str, obj_types);

        client.save2file(f, times, results);
    }


}
