package org.heigit.hosm.example;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHNode;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHWay;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMNode;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMWay;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Rtroilo on 1/19/17.
 * Revised by Jiaoyan on 1/21/17.
 */
public class HOSMClient {

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

    public static class MyJobResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<Long, Long> timestampCount;

        public MyJobResult(final Map<Long, Long> tc) {
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
            return new MyJobResult(result);
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

    public static class MyTaskAdapter extends ComputeTaskAdapter<JobOption, MyJobResult> {

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

}
