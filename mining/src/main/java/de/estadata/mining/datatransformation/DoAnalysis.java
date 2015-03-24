package de.estadata.mining.datatransformation;

import de.estadata.mining.graphmodel.*;
import de.estadata.mining.stclustering.STFiltering;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class DoAnalysis {
	private Cache pointsCache;
	private Cache clustersCache;
	private STFiltering filtering;
	private int maxSpaceDist;
	private int maxTimeDist;
	
	public DoAnalysis(List<Integer> reportIDs, 
			CacheManager databaseManager, 
			String pointsCacheDB, 
			String clustersCacheDB, 
			int spatialDistance, 
			int temporalDistance) throws IOException, InterruptedException, ExecutionException {
		if(spatialDistance < 0 || temporalDistance < 0)
			throw new IllegalArgumentException("Invalid parameters: " + spatialDistance + " meters, " + temporalDistance + " days");
		initializeVariables(databaseManager, pointsCacheDB, clustersCacheDB);
		
		filtering = new STFiltering(reportIDs, databaseManager, pointsCache, spatialDistance, temporalDistance);
	}
	
	public DoAnalysis(CacheManager databaseManager, 
			String pointsCacheDB, 
			String clustersCacheDB) throws IOException, InterruptedException, ExecutionException {
		initializeVariables(databaseManager, pointsCacheDB, clustersCacheDB);
		this.maxSpaceDist = Integer.MAX_VALUE;
		this.maxTimeDist = Integer.MAX_VALUE;
		filtering = new STFiltering(databaseManager, pointsCache, true);
	}
	
	private void initializeVariables(CacheManager databaseManager, 
			String pointsCacheDB, 
			String clustersCacheDB) throws IOException, InterruptedException, ExecutionException  {
		
		this.pointsCache = databaseManager.getCache(pointsCacheDB);
		if(pointsCache == null)
			throw new IllegalStateException("The specified database '" + pointsCacheDB + "' could not be loaded.");
		this.clustersCache = databaseManager.getCache(clustersCacheDB);
		if(clustersCache == null)
			throw new IllegalStateException("The specified database '" + clustersCacheDB + "' could not be loaded.");
		
	}
	
	/**
	 * Recommendation of st-similar reports for a single 'center report'. Executes a DFS starting on the center report and traverses only through the edges
	 * that are labeled with spatial/temporal distances that are lower or equal to those given as parameters of this method (similar holds for category).
	 * @param centerReport The report where the search begins in the st-graph.
	 * @param spatialDistance the maximum spatial distance (in meters) two reports can share. Cannot be bigger than the maximum spatial distance used in the constructor.
	 * @param temporalDistance the maximum temporal distance (in day) two reports can share.  Cannot be bigger than the maximum temporal distance used in the constructor.
	 * @param mustShareCategory if true, reports must furthermore all share the same category.
	 * @return The list of reports that fulfills the conditions specified by the parameters.
	 */
	public List<Report> recommendation(Report centerReport, int spatialDistance, int temporalDistance, boolean mustShareCategory) {
		if(spatialDistance < 0 || spatialDistance > maxSpaceDist || temporalDistance < 0 || temporalDistance > maxTimeDist) 
			throw new IllegalArgumentException("One or more of the spatio/temporal arguments is invalid");
		Graph graph = filtering.getGraph();
		if(graph.getNode(centerReport.getID()) == null)
			throw new IllegalArgumentException("No node corresponding to the given report could be found");
		
		List<Report> recommendation = new ArrayList<Report>();
		Map<Integer, Integer> visitedNodes = new HashMap<Integer, Integer>();
		LinkedList<Node> queue = new LinkedList<Node>();
		
		queue.add(graph.getNode(centerReport.getID()));
		while(!queue.isEmpty()) {
			Node n1 = queue.poll();
			if(!visitedNodes.containsKey(n1.getID())) {
				visitedNodes.put(n1.getID(), n1.getID());
				for(Integer id : n1.getAdjacentEdgesIDs()) {
					Edge e = graph.getEdge(id);
					if(mustShareCategory && !e.isSameCategory())
						continue;
					int spaceDist = e.getSpaceDist();
					int timeDist = e.getTimeDist();
					
					if(spaceDist <= spatialDistance && timeDist <= temporalDistance) {
						queue.add(graph.getNode(e.getOtherEndID(n1)));
					}
				}
			}
		}
		
		for(Entry<Integer, Integer> entry : visitedNodes.entrySet()) {
			if(entry.getKey() != centerReport.getID()) {
				recommendation.add((Report)pointsCache.get(entry.getKey()).getObjectValue());
			}
		}
		
		return recommendation;
	}
	
	/**
	 * Generates clusters with the given parameters by "cutting" the edges that do not comply with these new constraints. 
	 * The resulting connected components are then labeled with unique cluster IDs. The changes are reflected on the main Cache (where only reports are stored)
	 * and the resulting set of clusters is written in the clusters Cache (both caches were specified at the moment of construction). 
	 * Older cluster objects in the respective cache are deleted by the execution of this method.
	 * @param spatialDistance in meters
	 * @param temporalDistance in days
	 * @param mustShareCategory whether resulting connections should be between two reports sharing the same category. 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void doClustering(int spatialDistance, int temporalDistance, boolean mustShareCategory) throws InterruptedException, ExecutionException {
		GraphView view = filtering.filter(spatialDistance, temporalDistance, mustShareCategory);
		filtering.generateAndTransferClusters(clustersCache, view);
	}
	
	public STFiltering getFiltering() {
		return filtering;
	}
}

