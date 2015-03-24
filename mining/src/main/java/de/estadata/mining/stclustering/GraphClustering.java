package de.estadata.mining.stclustering;

import de.estadata.mining.modularityoptimizer.ModularityOptimizer;
import de.estadata.mining.modularityoptimizer.ModularityOptimizer.Algorithm;
import de.estadata.mining.modularityoptimizer.ModularityOptimizer.ModularityFunction;
import de.estadata.mining.scan.SCAN;
import de.estadata.mining.util.MiningTools;
import de.estadata.mining.datatransformation.Cluster;
import de.estadata.mining.datatransformation.Report;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import de.estadata.mining.graphmodel.*;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class provides 4 different kinds of clustering algorithms for graphs: SCAN, Louvain, Louvain with multilevel refinement and SLM.
 * For information about SCAN, see the {@link SCAN}. Regarding the other three, see {@link ModularityOptimizer}.
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 * 
 */
public class GraphClustering {
	private Graph graph;

	private Cache pointsCache;
	private Cache clustersCache;

	private ModularityOptimizer modOpt;

	/**
	 * Constructor for a new instance with an already-built graph.
	 * @param graph the graph.
	 * @param pointsCache the Terracotta cache containing the {@link Report} instances.
	 * @param clustersCache the Terracotta cache where the {@link Cluster} instances are to be stored.
	 */
	public GraphClustering(Graph graph, Cache pointsCache, Cache clustersCache) {
		this.graph = graph;

		this.pointsCache = pointsCache;
		this.clustersCache = clustersCache;
	}

	/**
	 * Execute SCAN. The resulting graph can be obtained by
	 * calling getGraph() after the execution of this method.
	 */
	public void runSCAN(double epsilon, int mu) {
		System.out.println("Starting clustering...");
		SCAN scan = new SCAN(this.graph, epsilon, mu);

		long start = System.currentTimeMillis();
		int finalID = scan.run(1);
		long end = System.currentTimeMillis();

		this.graph = scan.getGraph();
		
		Map<Integer, List<Integer>> clustersMap = scan.getClustersMap();
		int validClusters = 0;
		for(Integer id : clustersMap.keySet()) {
			if(id == -1)
				continue;
			if(clustersMap.get(id).size() > 1)
				validClusters++;
		}

		System.out.println("Finished clustering using SCAN after: " + (end - start)
				+ " ms, number of clusters: " + finalID + ", #valid clusters: " + validClusters);
	}
	
	/**
	 * Run a modularity-based clustering algorithm on the current graph. For full information about the parameters, see
	 * {@link ModularityOptimizer} or visit http://www.ludowaltman.nl/slm/.
	 * @param modFunc
	 * @param resolution
	 * @param algorithm
	 * @param randomStarts
	 * @param iterations
	 * @param randomSeed
	 * @throws IllegalStateException
	 */
	public void runModularityOptimizer(ModularityFunction modFunc, 
			double resolution, 
			Algorithm algorithm, 
			int randomStarts, 
			int iterations, 
			long randomSeed) throws IllegalStateException {
		
		if(modOpt == null || !modOpt.getGraph().equals(graph)) {
			modOpt = new ModularityOptimizer(graph);
		}
		
		modOpt.run(modFunc, resolution, algorithm, randomStarts, iterations, randomSeed);
	}
	
	/**
	 * Exports the current graph to csv.
	 * @param pointsDestFilePath destination file for the graph.
	 * @param clustersDestFilePath destination file for the clusters.
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public void exportGraphToCSV(String pointsDestFilePath,
			String clustersDestFilePath) throws FileNotFoundException,
			UnsupportedEncodingException {
		List<Report> reports = MiningTools.getCacheObjectsAsList(pointsCache,
				Report.class, null);
		List<Cluster> clusters = (clustersCache == null) ? null : MiningTools.getCacheObjectsAsList(clustersCache, Cluster.class, null);
		MiningTools.resultsToCSV(reports, pointsDestFilePath,
				clustersDestFilePath, clusters);
	}

	/**
	 * This method should be executed once some clustering algorithm was executed (i.e. after runSCAN() or runModularityOptimizer()).
	 * It generates the {@link Cluster} instances corresponding to the cluster IDs assigned to the nodes in the graph and transfers
	 * them to the Terracotta clusters cache (provided in the constructor).
	 */
	public void generateAndTransferClusters() {
		Map<Integer, List<Report>> clusterMap = new HashMap<Integer, List<Report>>();

		for (int id : graph.getNodeIDs()) {
			Node n = graph.getNode(id);
//			int clusterID = Integer.parseInt(MiningTools.getNodeLabelValue(n, clusterIDLabel));
			int clusterID = n.getClusterID();
			if(clusterID == Integer.MIN_VALUE)
				throw new IllegalStateException("Failed to assign valid cluster ID to at least one node");
			Report report = (Report) pointsCache.get(id).getObjectValue();
			report.setClusterID(clusterID);
			pointsCache.put(new Element(id, report));

			MiningTools.addReportToClustermap(report, clusterMap);
		}
		
		int biggestCluster = Integer.MIN_VALUE;
		int smallestCluster = Integer.MAX_VALUE;
		for (Integer key : clusterMap.keySet()) {
			if(key == -1) 
				continue;
			List<Report> list = clusterMap.get(key);
			if(list.size() == 1) {
				Report rep = list.get(0);
				rep.setClusterID(-1);
			} else {
				Cluster cluster = new Cluster(list, key);
				clustersCache.put(new Element(key, cluster));
				
				if(list.size() > biggestCluster)
					biggestCluster = list.size();
				else if(list.size() < smallestCluster)
					smallestCluster = list.size();
			}
		}
		System.out.println("clustersCache.size = " + clustersCache.getKeys().size());
		System.out.println("Biggest cluster has " + biggestCluster + " nodes; smallest cluster has " + smallestCluster + " nodes");
	}

	/**
	 * Returns the graph instance.
	 * @return the graph.
	 */
	public Graph getGraph() {
		return graph;
	}
}
