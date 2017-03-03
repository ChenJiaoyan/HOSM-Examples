
package org.heigit.hosm.example;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
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
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMWay;

import javax.cache.Cache;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Rtroilo on 1/19/17.
 * Revised by Jiaoyan on 1/21/17.
 * Latest update: Jan 23, 2017
 */
public class HOSMClient_Shops {


    public static class JobOption implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<Long> timestamps;
        private final Geometry bbox;
        private final int tagKey;
        private final int tagValue;

        public JobOption(final List<Long> timestamps, final Geometry bbox, final int tagKey) {
            this.timestamps = timestamps;
            this.bbox = bbox;
            this.tagKey = tagKey;
            this.tagValue = -1;
        }

        public JobOption(final List<Long> timestamps, final Geometry bbox, final int tagKey, final int tagValue) {
            this.timestamps = timestamps;
            this.bbox = bbox;
            this.tagKey = tagKey;
            this.tagValue = tagValue;
        }
    }

    public static class JobResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<Long, Long> timestampCount;

        public JobResult(final Map<Long, Long> tc) {
            this.timestampCount = tc;
        }
    }

    public static class CountJob extends ComputeJobAdapter {
        private static final long serialVersionUID = 1L;
        private final JobOption option;

        @IgniteInstanceResource
        private Ignite ignite;
        private final boolean localMode;

        private String[] object_types;

        public CountJob(JobOption option) {
            this.option = option;
            this.localMode = true;
            this.object_types = new String[]{"way"};
        }

        public CountJob(final JobOption option, Ignite ignite, final boolean localMode) {
            this.option = option;
            this.localMode = localMode;
            this.ignite = ignite;
            this.object_types = new String[]{"way"};
        }

        public CountJob(final JobOption option, Ignite ignite, final boolean localMode, String[] object_types) {
            this.option = option;
            this.localMode = localMode;
            this.ignite = ignite;
            this.object_types = object_types;
        }

        @Override
        public Object execute() throws IgniteException {
            Map<Long, Long> result = new HashMap<>(option.timestamps.size());
            for (int i = 0; i < this.object_types.length; i++) {
                switch (object_types[i]) {
                    case "way":
                        result = countWay(result);
                        break;
                    case "node":
                        result = countNode(result);
                        break;
                    default:
                        break;
                }
            }
            return new JobResult(result);
        }

        private Map<Long, Long> countWay(Map<Long, Long> result) {
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
                        if (hasKeyValue(way.getTags(), option.tagKey, option.tagValue)) {
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
            return result;
        }

        private Map<Long, Long> countNode(Map<Long, Long> result) {
            IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");
            SqlQuery<AffinityKey<Long>, OSHNode> sqlNode = new SqlQuery<>(OSHNode.class, "BoundingBox && ?");
            sqlNode.setArgs(option.bbox);
            sqlNode.setLocal(localMode);

            try (QueryCursor<Cache.Entry<AffinityKey<Long>, OSHNode>> cursor = cacheNode.query(sqlNode)) {
                for (Cache.Entry<AffinityKey<Long>, OSHNode> row : cursor) {
                    OSHNode oshNode = row.getValue();
                    Map<Long, OSMNode> timestampNodeMap = oshNode.getByTimestamp(option.timestamps);
                    for (Map.Entry<Long, OSMNode> timestampNode : timestampNodeMap.entrySet()) {
                        Long timestamp = timestampNode.getKey();
                        OSMNode node = timestampNode.getValue();
                        if (hasKeyValue(node.getTags(), option.tagKey, option.tagValue)) {
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
            return result;
        }

        /*
        * tags is an index array of [key,value, key,value, ...] order by key!
        */
        private boolean hasKeyValue(int[] tags, int key, int value) {
            for (int i = 0; i < tags.length; i += 2) {
                if (tags[i] < key)
                    continue;
                if (tags[i] == key) {
                    if (value == -1) {
                        return true;
                    } else {
                        if (tags[i + 1] == value) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                return false;
            }
            return false;
        }
    }

    public static class MyTaskAdapter extends ComputeTaskAdapter<JobOption, JobResult> {

        private static final long serialVersionUID = 1L;

        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, JobOption arg)
                throws IgniteException {

            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

            CountJob myJob = new CountJob(arg);
            // for every node in the cluster!
            for (ClusterNode node : subgrid) {
                map.put(myJob, node);
            }
            return map;
        }

        @Override
        public JobResult reduce(List<ComputeJobResult> results) throws IgniteException {
            Map<Long, Long> reducedResult = new HashMap<>();
            for (ComputeJobResult computeJobResult : results) {
                JobResult rs = computeJobResult.<JobResult>getData();
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
            return new JobResult(reducedResult);
        }

    }


    public Map<Long,Long> spatial_temporal_count(String tagKey, String tagValue, ArrayList<Long> times_arr,
                                                 String polygon_str) throws ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        String[] obj_types = new String[]{"way"};
        return spatial_temporal_count(tagKey, tagValue, times_arr, polygon_str, obj_types);
    }
    public Map<Long,Long> spatial_temporal_count(String tagKey, ArrayList<Long> times_arr, String polygon_str,
                                                 String[] obj_types) throws ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        return spatial_temporal_count(tagKey, null, times_arr, polygon_str, obj_types);
    }

    public Map<Long,Long> spatial_temporal_count(String tagKey, ArrayList<Long> times_arr, String polygon_str
    ) throws ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        String[] obj_types = new String[]{"way"};
        return spatial_temporal_count(tagKey, null, times_arr, polygon_str, obj_types);
    }

    public Map<Long,Long> spatial_temporal_count(String tagKey, String tagValue, ArrayList<Long> times_arr, String polygon_str,
                                                 String[] obj_types) throws ParseException, com.vividsolutions.jts.io.ParseException, IgniteCheckedException {
        Ignition.setClientMode(true);
        IgniteConfiguration icfg = IgnitionEx.loadConfiguration("ignite.xml").getKey();
        try (Ignite ignite = Ignition.start(icfg)) {
            int[] tags = get_tag_value(ignite, tagKey, tagValue);
            if (tags != null) {
                int tag_k_n = tags[0];
                int tag_v_n = tags[1];
                List<Long> timestamps = times_arr;
                WKTReader r = new WKTReader();
                Geometry bbox = r.read(polygon_str);
                JobOption option = new JobOption(timestamps, bbox, tag_k_n, tag_v_n);
                IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());
                CountJob myJob = new CountJob(option, ignite, false, obj_types);
                JobResult result = (JobResult) myJob.execute();
                return result.timestampCount;
            } else {
                return null;
            }
        }
    }


    private int[] get_tag_value(Ignite ignite, String tagKey, String tagValue) {
        int tag_v_n = -1;
        IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
        List<List<?>> rows = cacheTags
                .query(new SqlFieldsQuery("select _key,values from OSMTag where key = ?").setArgs(tagKey)).getAll();
        if (rows == null || rows.isEmpty()) {
            System.err.println("Tags with key building not found!");
            return null;
        }
        int tag_k_n = ((Integer) rows.get(0).get(0)).intValue();
        System.out.printf("tag key: %d \n", tag_k_n);

        Object[] values = (Object[]) rows.get(0).get(1);
        for (int i = 0; i < values.length; i++) {
            if (((String) values[i]).equals(tagValue)) {
                tag_v_n = i;
                break;
            }
        }
        System.out.printf("%s \n", Arrays.toString((Object[]) rows.get(0).get(1)));
        System.out.printf("tag value: %d \n", tag_v_n);

        return new int[]{tag_k_n, tag_v_n};
    }

}
