package org.heigit.hosm.example;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
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
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHNode;
import org.heigit.bigspatialdata.osh.ignite.model.osh.OSHWay;
import org.heigit.bigspatialdata.osh.ignite.model.osh.api.OSHWayAPI;
import org.heigit.bigspatialdata.osh.ignite.model.osm.OSMTag;


import javax.cache.Cache;
import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {

	static SqlFieldsQuery queryTags = new SqlFieldsQuery("select key, values[?] from OSMTag where _key = ?");
		
	
	static class MyBroadcast implements IgniteRunnable, Serializable {
		private static final long serialVersionUID = 1L;
		@IgniteInstanceResource
		Ignite ignite;
		
		//@Override
		public void run() {
			 System.out.println(">>> Hello Node: " + ignite.cluster().localNode().id());
		}
	}
	
	static class MyTaskResult implements Serializable{
		private static final long serialVersionUID = 1L;
		private final int numNodes;
		private final int numWays;
		
		public MyTaskResult(final int numNodes, final int numWays){
			this.numNodes = numNodes;
			this.numWays = numWays;
		}

		public int getNumNodes() {
			return numNodes;
		}

		public int getNumWays() {
			return numWays;
		}
		
	
	}
	
	static class MyComputeJob extends ComputeJobAdapter{
		private static final long serialVersionUID = 1L;
		private final Geometry geometry;
		
		@IgniteInstanceResource
		private Ignite ignite;
		
		public MyComputeJob(final Geometry geometry){
			this.geometry = geometry;
		}

		@Override
		public Object execute() throws IgniteException {
			IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");
        	SqlQuery<AffinityKey<Long>, OSHNode> sqlNode = new SqlQuery<>(OSHNode.class, "BoundingBox && ?");
        	sqlNode.setArgs(geometry);
        	sqlNode.setLocal(true);
        	
        	
        	IgniteCache<AffinityKey<Long>, OSHWay> cacheWay = ignite.cache("osm_way");
        	SqlQuery<AffinityKey<Long>, OSHWay> sqlWay = new SqlQuery<>(OSHWay.class, "BoundingBox && ?");
        	sqlWay.setArgs(geometry);
        	sqlWay.setLocal(true);
        	
        	int numNodes = 0;
        	try(QueryCursor<Cache.Entry<AffinityKey<Long>,OSHNode>> cursor  =  cacheNode.query(sqlNode)){
				for(Cache.Entry<AffinityKey<Long>,OSHNode> entry : cursor){
					numNodes++;
				}
			}
        	int numWays = 0;
        	try(QueryCursor<Cache.Entry<AffinityKey<Long>,OSHWay>> cursor  =  cacheWay.query(sqlWay)){
				for(Cache.Entry<AffinityKey<Long>,OSHWay> entry : cursor){
					numWays++;
				}
			}
        	
			return new MyTaskResult(numNodes, numWays);
		}
	}
	
	static class MyTask extends ComputeTaskAdapter<Geometry, MyTaskResult> {

		private static final long serialVersionUID = 1L;
	
		
		@Override
		public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Geometry arg)
				throws IgniteException {

			
			Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());
			MyComputeJob myJob = new MyComputeJob(arg);
			
			for(ClusterNode node : subgrid){
				map.put(myJob, node);
			}
			
			return map;
		}

		@Override
		public MyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
			int numNodes = 0;
			int numWays = 0;
			
			for(ComputeJobResult res : results){
				MyTaskResult tr = res.<MyTaskResult>getData();
				
				numNodes += tr.getNumNodes();
				numWays += tr.getNumWays();
			}
			
			return new MyTaskResult(numNodes, numWays);
		}
		
	}
	
	
	public static void main(String[] args) throws ParseException, IgniteCheckedException, URISyntaxException {
		Ignition.setClientMode(true);
        String dir = MLib_Test.class.getResource("/").toURI().getPath() + "../../";
		File f = new File(dir + "ignite.xml");
        System.out.println(f.exists());
		IgniteConfiguration icfg = IgnitionEx.loadConfiguration(dir + "ignite.xml").getKey();
		
		try(Ignite ignite = Ignition.start(icfg)){
			
			IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());
			
			

			// Print out hello message on remote nodes in projection.
			//compute.broadcast(new MyBroadcast());
			
			
			WKTReader r = new WKTReader();
			Geometry g = r.read("POLYGON((12.357822060585022 45.42796074630555,12.358822524547577 45.42796074630555,12.358822524547577 45.427420498069445,12.357822060585022 45.427420498069445,12.357822060585022 45.42796074630555))");
		
			MyTaskResult result = compute.execute(MyTask.class, g);
			
			System.out.printf("Found:\nNumNodes %d\nNumWays %d\n",result.getNumNodes(),result.getNumWays());
			
			
			/*
			IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");
			SqlFieldsQuery explain = new SqlFieldsQuery("explain select version from OSHNode where BoundingBox && ? and location && ?");
			
			explain.setArgs(g,g);
			
			List<List<?>> explainReluslt = cacheNode.query(explain).getAll();
			
			for(List<?> row : explainReluslt){
				System.out.println(row.get(0));
			}
			
			
			
			
			*/
			
			/*
			IgniteCache<Integer, OSMTag> cacheTags = ignite.cache("osm_tags");
			
			IgniteCache<AffinityKey<Long>, OSHWay> cacheWay = ignite.cache("osm_way");
			
			
			SqlQuery<AffinityKey<Long>, OSHNode> sql = new SqlQuery<>(OSHNode.class, "BoundingBox && ? and location && ?");
			SqlQuery<AffinityKey<Long>, OSHWay> sqlWay = new SqlQuery<>(OSHWay.class, "BoundingBox && ?");
			
			
			sql.setArgs(g,g);
			IgniteCache<AffinityKey<Long>, OSHNode> cacheNode = ignite.cache("osm_node");		
			try(QueryCursor<Cache.Entry<AffinityKey<Long>,OSHNode>> cursor  =  cacheNode.query(sql)){
				for(Cache.Entry<AffinityKey<Long>,OSHNode> entry : cursor){
					System.out.println(entry.getValue());
				}
			}
			
			*/
			
			/*
			try(QueryCursor<Cache.Entry<AffinityKey<Long>,OSHWay>> cursor  =  cacheWay.query(sqlWay.setArgs(g))){
				for(Cache.Entry<AffinityKey<Long>,OSHWay> entry : cursor){
					System.out.println(entry.getValue());
				}
			}
			
			
			
			*/
				
			/*
			IgniteCache<AffinityKey<Long>, OSHWay> cacheWay = ignite.cache("osm_way");
			
			SqlQuery<AffinityKey<Long>, OSHWay> sqlWay = new SqlQuery<>(OSHWay.class, "BoundingBox && ?");
			
			
			//w1.getByTimestamp(Arrays.asList(1331823278001l));
			
			OSHWay w1 = cacheWay.get(new AffinityKey<Long>(129408157l)); // --> Node 29499377l));
			System.out.println(w1);
			
			
			for(Map.Entry<Long, OSMWay> version : w1.getByTimestamp(Arrays.asList(1315563142001l,1315563142002l,1331823278001l)).entrySet()){
				System.out.printf("TS:%d -> %s\n", version.getKey(), version.getValue());
				//printFull(version.getValue(), cacheTags);
			}
			
			*/
		}

	}
	
	public static String[] getTags(int[] t,IgniteCache<Integer, OSMTag> cacheTags){
		if(t.length == 0)
			return new String[0];
		
		String[] tags = new String[t.length];
		for(int i=0; i < t.length; i+=2){
			List<List<?>> rows = cacheTags.query(queryTags.setArgs(t[i+1],t[i])).getAll();
			if(rows.isEmpty())
				continue;
			List<?> row = rows.get(0);
			tags[i] = (String) row.get(0);
			tags[i+1] = (String) row.get(1);
		}
		
		return tags;
	}

	public static void printFull(OSHWayAPI n, IgniteCache<Integer, OSMTag> cacheTags){
		String entity = String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s USER:%d TAGS:%S",-1, n.getVersion(), n.getTimestamp(), n.getChangeset(),n.isVisible(),n.getUserId(),Arrays.toString(getTags(n.getTags(),cacheTags)));
		String.format("WAY-> %s Refs:%s BBox:%s",entity, Arrays.toString(n.getRefs()), n.getBoundingBox());
		System.out.println(String.format("WAY-> %s Refs:%s BBox:%s",entity, Arrays.toString(n.getRefs()), n.getBoundingBox()));
	}
	
}
