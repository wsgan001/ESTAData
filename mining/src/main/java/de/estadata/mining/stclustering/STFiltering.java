package de.estadata.mining.stclustering;

import de.estadata.mining.datatransformation.Cluster;
import de.estadata.mining.datatransformation.Report;
import de.estadata.mining.graphmodel.*;
import de.estadata.mining.util.DirectMemoryUtils;
import de.estadata.mining.util.MiningTools;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.SearchAttribute;
import net.sf.ehcache.config.Searchable;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This class is the core of the analysis. It provides the implementation for generating a {@link Graph} instance out of a set
 * of {@link Report} instances, as well as loading a previously existent Graph. This graph is generated accordingly to the 
 * workbench defined in [Improving Participatory Urban Infrastructure Monitoring through Spatio-Temporal Analytics, by Borges et al]. 
 * We refer to it for the readers interested in the theoretical background.
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 * 
 */
public class STFiltering {
	public static final String reportIDColumn = "id";
//	public static final String clusterIDLabel = "clusterID";
	private int maxSpaceDist;
	private int maxDayDist;
	private Graph graph;
	private long reportsStartTime;
	private long reportsEndTime;
	private long reportsTimeSpan;
	private CacheManager databaseManager;
	private Cache reportsCache;
	private Cache clustersCache;
//	private Cache[] bucketCaches;
	private Map<Integer, List<Integer>> buckets;
	private long initialFreeHeap = 0;
	private long initialDirectMemory = 0;
	private long reportsCacheHeap = 0;
	private long nodesHeap = 0;
	private long edgesHeap = 0;
	private long reportsCacheRAM = 0;
	private long nodesRAM = 0;
	private long edgesRAM = 0;

	/**
	 * Constructs a new instance from a specific set of {@link Report}s, defined by a list containing their IDs.
	 * @param reportIDs the list of IDs from the reports to be analyzed.
	 * @param databaseManager the Terracotta CacheManager 
	 * @param reportsCache the Terracotta Cache containing the reports.
	 * @param maxSpaceDist maximal space distance (in meters) two Reports may have to be ST-connected.
	 * @param maxDayDist maximal temporal distance (in days) two Reports may have to be ST-connected.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public STFiltering(List<Integer> reportIDs, CacheManager databaseManager,
			Cache reportsCache, int maxSpaceDist, int maxDayDist)
			throws InterruptedException, ExecutionException {
		initializeVariables(databaseManager, reportsCache, false);
		this.maxSpaceDist = maxSpaceDist;
		this.maxDayDist = maxDayDist;
		loadFromReportIDs(reportIDs);
		generateGraph();
	}

	/**
	 * Constructs a new instance using the complete set of {@link Report}s provided in a given Terracotta cache.
	 * @param databaseManager the Terracotta CacheManager.
	 * @param reportsCache the Terracotta Cache containing the reports.
	 * @param maxSpaceDist maximal space distance (in meters) two Reports may have to be ST-connected.
	 * @param maxDayDist maximal temporal distance (in days) two Reports may have to be ST-connected.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public STFiltering(CacheManager databaseManager, Cache reportsCache,
			int maxSpaceDist, int maxDayDist) throws InterruptedException,
			ExecutionException {
		initializeVariables(databaseManager, reportsCache, false);
		this.maxSpaceDist = maxSpaceDist;
		this.maxDayDist = maxDayDist;
		loadFromReportIDs(null);
		generateGraph();

	}

	/**
	 * Constructs a new instance in one of two possible ways: by using a previously generated Graph structure or by
	 * generating a new graph using all available {@link Report}s in the given Terracotta and using default values 
	 * for the maximal space and temporal distance (100m and 30 days, respectively). Note that, in the case of using a previously
	 * generated graph structure, the provided CacheManager should contain it in the form of two Caches with names
	 * '[reportsCache.name]_nodesCache' and '[reportsCache.name]_edgesCache'. These are the names used every time a new graph
	 * is generated, and therefore the program assumes that they are still there when attempting to re-load the graph on a later
	 * occasion.
	 * @param databaseManager the Terracotta CacheManager.
	 * @param reportsCache the Terracotta Cache containing the reports.
	 * @param useExistingGraphStructure if set to true, load previously generated {@link Graph}, if set to false, generate a new one.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public STFiltering(CacheManager databaseManager, Cache reportsCache,
			boolean useExistingGraphStructure) throws InterruptedException,
			ExecutionException {
		initializeVariables(databaseManager, reportsCache,
				useExistingGraphStructure);
		if (useExistingGraphStructure) {
			checkForConsistency();
		} else {
			loadFromReportIDs(null);
			generateGraph();
		}
	}

	/**
	 * Execute the ST-filtering step using the maxSpace and maxTime values,
	 * which were either set in one of the constructors or the defaults were
	 * used. This method only changes the intern structure of the graph (i.e.
	 * only new edges appear, but the number of nodes remain the same). The
	 * reports stored in the database are not affected by the execution of this
	 * method.
	 * 
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	private void generateGraph() throws InterruptedException,
			ExecutionException {
		System.out.println("Starting to generate graph...");
		long start = System.currentTimeMillis();
		computeNeighbors();
		long end = System.currentTimeMillis();
		System.out.println("Finished generating graph with "
				+ graph.getNodeCount() + " nodes and " + graph.getEdgeCount()
				+ " edges after " + (end - start) + " ms");
		cleanupBuckets();
	}

	/**
	 * Filter graph with stricter constraints. Returns a {@link GraphView} containing the resulting nodes and edges after the filtering.
	 * @param newMaxSpaceDist new maximal spatial distance in meters. Must be in range [0,maxSpaceDist], where maxSpaceDist is the value set when constructing the object.
	 * @param newMaxDayDist new maximal temporal distance in days. Must be in range [0,maxDayDist], where maxDayDist is the value set when constructing the object.
	 * @param mustShareCategory if set to true, only edges connecting nodes of the same category are returned.
	 * @return a GraphView containg all nodes and edges remaining after the filtering.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public GraphView filter(double newMaxSpaceDist, int newMaxDayDist,
			boolean mustShareCategory) throws InterruptedException,
			ExecutionException {
		if (newMaxSpaceDist < 0 || newMaxSpaceDist > this.maxSpaceDist)
			throw new IllegalArgumentException(
					"Invalid value for first argument. Must have a value between 0 and "
							+ this.maxSpaceDist);

		if (newMaxDayDist < 0 || newMaxDayDist > this.maxDayDist)
			throw new IllegalArgumentException(
					"Invalid value for second argument. Must have a value between 0 and "
							+ this.maxDayDist);

		System.out
				.println("Applying filtering to graph with new maxSpaceDist: "
						+ newMaxSpaceDist + " meters, new maxTempDist: "
						+ newMaxDayDist + " days and mustShareCategory: "
						+ mustShareCategory);
		long start = System.currentTimeMillis();

		GraphView view = new GraphView(graph);
		view.addAllNodeIDs(graph.getNodeIDs());

		for (int id : graph.getEdgeIDs()) {
			Edge e = graph.getEdge(id);
			if (mustShareCategory && !e.isSameCategory()) {
				continue;
			} else if (e.getSpaceDist() <= newMaxSpaceDist && e.getTimeDist() <= newMaxDayDist)
				view.addEdgeID(e.getID());
		}
		long end = System.currentTimeMillis();
		System.out.println("Filtering took: " + (end - start) + " ms");

		System.out.println("Original graph has " + graph.getNodeCount()
				+ " nodes and " + graph.getEdgeCount() + " edges");
		System.out.println("Filtered graph hast " + view.getNodeIDs().size()
				+ " nodes and " + view.getEdgeIDs().size() + " edges");

		return view;
	}
	
	/**
	 * Labels the nodes of every connected component with a single, unique,
	 * positive cluster ID, as long as said component consists of 2 or more
	 * nodes. Isolated nodes (i.e. nodes without any incident edges) are labeled
	 * with the cluster ID '-1'. The labeling of a node with a cluster ID is
	 * reflected in the corresponding report, which is stored in the main Cache.
	 * Reports with the same cluster ID are then gathered under a Cluster
	 * instance and stored in the cluster Cache.
	 * 
	 * @param clustersCache
	 *            The cache where the Cluster instances are going to be stored.
	 */
	public void generateAndTransferClusters(Cache clustersCache) {
		generateAndTransferClusters(clustersCache, null);
	}

	/**
	 * Labels the nodes of every connected component (given by the {@link GraphView}) with a single, unique,
	 * positive cluster ID, as long as said component consists of 2 or more
	 * nodes. Isolated nodes (i.e. nodes without any incident edges) are labeled
	 * with the cluster ID '-1'. The labeling of a node with a cluster ID is
	 * reflected in the corresponding report, which is stored in the main Cache.
	 * Reports with the same cluster ID are then gathered under a Cluster
	 * instance and stored in the cluster Cache.
	 * 
	 * @param clustersCache the cache where the Cluster instances are going to be stored.
	 * @param view the GraphView instance containing the sets of node and edge IDs to be taken into account.
	 */
	public void generateAndTransferClusters(Cache clustersCache, GraphView view) {
		long start = System.currentTimeMillis();
		MiningTools.assignClusterIDsOnGraph(graph, view);
		Map<Integer, List<Report>> clusterMap = new HashMap<Integer, List<Report>>();
		for (Integer id : graph.getNodeIDs()) {
			if (view == null || view.containsNodeID(id)) {
				Node n = graph.getNode(id);
				int clusterID = n.getClusterID();
				if(clusterID == Integer.MIN_VALUE)
					throw new IllegalStateException("Failed to assign a valid cluster ID to at least one node");
				Report report = (Report) reportsCache.get(id).getObjectValue();
				report.setClusterID(clusterID);
				reportsCache.put(new Element(id, report));
				MiningTools.addReportToClustermap(report, clusterMap);
			}
		}
		long time = System.currentTimeMillis() - start;
		System.out.println("Assignment of clusterIDs to nodes & reports took " + time + " ms");
		clustersCache.removeAll();
		for (Integer key : clusterMap.keySet()) {
			Cluster cluster = new Cluster(clusterMap.get(key), key);
			Element element = new Element(key, cluster);
			clustersCache.put(element);
		}
		this.clustersCache = clustersCache;
	}

	/**
	 * Exports graph and clusters to csv files.
	 * @param pointsDestFilePath destination file of the graph.
	 * @param clustersDestFilePath destination file of the clusters.
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public void exportToCSV(String pointsDestFilePath,
			String clustersDestFilePath) throws FileNotFoundException,
			UnsupportedEncodingException {
		List<Report> reports = MiningTools.getCacheObjectsAsList(reportsCache,
				Report.class, null);
		List<Cluster> clusters = (clustersCache == null) ? null : MiningTools.getCacheObjectsAsList(clustersCache, Cluster.class, null);
		MiningTools.resultsToCSV(reports, pointsDestFilePath,
				clustersDestFilePath, clusters);
	}
	
	private void initializeVariables(CacheManager databaseManager,
			Cache reportsCache, boolean useExistingGraphStructure) {
		this.databaseManager = databaseManager;
		this.reportsCache = reportsCache;

		if (reportsCache == null)
			throw new NullPointerException();

		initialFreeHeap = getMaxAvailableHeapMemory();
		initialDirectMemory = DirectMemoryUtils.getDirectMemorySize();
		

		reportsCacheHeap = reportsCache.getCacheConfiguration()
				.getMaxBytesLocalHeap();
		reportsCacheRAM = reportsCache.getCacheConfiguration()
				.getMaxBytesLocalOffHeap();

		nodesHeap = (initialFreeHeap - reportsCacheHeap) / 4;
		edgesHeap = nodesHeap;

		nodesRAM = (initialDirectMemory - reportsCacheRAM) / 3;
		edgesRAM = nodesRAM;

		String nodesCacheName = reportsCache.getName() + "_nodesCache";
		String edgesCacheName = reportsCache.getName() + "_edgesCache";
		Cache nodesCache;
		Cache edgesCache;
		if (useExistingGraphStructure) {
			nodesCache = databaseManager.getCache(nodesCacheName);
			edgesCache = databaseManager.getCache(edgesCacheName);
			if (nodesCache == null || edgesCache == null) {
				throw new IllegalStateException(
						"Cannot continue: missing existing graph structure");
			}
		} else {
			Hashtable<String, String> nodeAttributes = new Hashtable<>();
			Hashtable<String, String> edgeAttributes = new Hashtable<>();
			
			nodeAttributes.put("id", "getID()");
			
			edgeAttributes.put("id", "getID()");
			edgeAttributes.put("sourceID", "getSourceID()");
			edgeAttributes.put("targetID", "getTargetID()");
			edgeAttributes.put("weight", "getWeight()");

			nodesCache = getNewCache(nodesCacheName, nodesHeap, nodesRAM, true, nodeAttributes);
			edgesCache = getNewCache(edgesCacheName, edgesHeap, edgesRAM, true, edgeAttributes);
		}

		graph = new Graph(nodesCache, edgesCache);

		maxSpaceDist = 100;
		maxDayDist = 30;
	}
	
	private void computeNeighbors() throws InterruptedException, ExecutionException {

		final int threads = Runtime.getRuntime().availableProcessors();
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		List<Future<List<Edge>>> futures = new ArrayList<Future<List<Edge>>>();
		Integer[] keys = buckets.keySet().toArray(new Integer[buckets.size()]);
		
		for (int i = 0; i < keys.length; i++) {
			Future<List<Edge>> f = pool.submit(new BucketThread(keys[i]));
			futures.add(f);
		}
		for (int i = 0; i + 1 < keys.length; i += 2) {
			Future<List<Edge>> f = pool.submit(new BucketThread(keys[i], keys[i + 1]));
			futures.add(f);
		}
		for (int i = 1; i + 1 < keys.length; i += 2) {
			Future<List<Edge>> f = pool.submit(new BucketThread(keys[i], keys[i + 1]));
			futures.add(f);
		}
		
		try {
			for (Future<List<Edge>> f : futures) {
				List<Edge> edges = f.get();
				if (edges.size() == 0) {
				} else {
					for (Edge e : edges)
						graph.addEdge(e);
				}
			}
		} catch (InterruptedException e) {
			pool.shutdownNow();
			throw e;
		} catch (ExecutionException e) {
			pool.shutdownNow();
			throw e;
		}
		
		pool.shutdown();
		
	}
	
	/*
	 * ##########################################################################
	 * # Auxiliary class for version 3.1 Instances of this class are going to be
	 * later on called in parallel.
	 */
	private class BucketThread implements Callable<List<Edge>> {

		// Indices pointing to the corresponding buckets in the bucket array
		private int firstCacheIndex = -1;
		private int secondCacheIndex = -1;

		public BucketThread(int firstCacheIndex) {
			if (firstCacheIndex < 0)
				throw new IllegalArgumentException();
			this.firstCacheIndex = firstCacheIndex;
		}

		public BucketThread(int firstCacheIndex, int secondCacheIndex) {
			if (firstCacheIndex < 0 || secondCacheIndex < 0)
				throw new IllegalArgumentException();
			this.firstCacheIndex = firstCacheIndex;
			this.secondCacheIndex = secondCacheIndex;
		}

		public List<Edge> call() throws Exception {
			long start = System.currentTimeMillis();
			List<Edge> edges = new ArrayList<Edge>();

			List<Report> firstCacheReports = MiningTools.getCacheObjectsAsList(reportsCache, Report.class, buckets.get(firstCacheIndex));
			firstCacheReports = (firstCacheReports == null) ? new ArrayList<Report>() : firstCacheReports;
			String msg;
			if (secondCacheIndex == -1) {
				edges = processReportsLists(firstCacheReports,
						firstCacheReports, true);
				msg = "Processing of cache " + firstCacheIndex + " ("
						+ firstCacheReports.size() + " reports) took: ";
			} else {
				List<Report> secondCacheReports = MiningTools.getCacheObjectsAsList(reportsCache, Report.class, buckets.get(secondCacheIndex));
				secondCacheReports = (secondCacheReports == null) ? new ArrayList<Report>() : secondCacheReports;
				edges = processReportsLists(firstCacheReports,
						secondCacheReports, false);
				msg = "Processing of caches ["
						+ firstCacheIndex
						+ ", "
						+ secondCacheIndex
						+ "] ("
						+ (firstCacheReports.size() + secondCacheReports.size())
						+ " reports) took: ";
			}

			long end = System.currentTimeMillis();
			msg += (end - start) + " ms";
			System.out.println(msg);
			return edges;
		}

		private List<Edge> processReportsLists(List<Report> l1,
				List<Report> l2, boolean sameList) {
			List<Edge> edges = new ArrayList<Edge>();
			for (int i = 0; i < l1.size(); i++) {
				Report r1 = l1.get(i);
				int j = (sameList) ? i + 1 : 0;
				for (; j < l2.size(); j++) {
					Report r2 = l2.get(j);
					int spaceDist = Math.abs(MiningTools.getSpaceDistance(r1, r2));
					if (spaceDist < maxSpaceDist) {
						long maxTemporalDist = TimeUnit.DAYS.toMillis(maxDayDist);
						long timeDist = Math.abs(r1.getCreationTime() - r2.getCreationTime());
						if (timeDist < maxTemporalDist) {
							Node n1 = graph.getNode(r1.getID());
							Node n2 = graph.getNode(r2.getID());
							boolean sameCategory = r1.getCategory().toLowerCase().equals(r2.getCategory().toLowerCase());
							
							Edge e = new Edge(n1, n2, MiningTools.getLevenshteinSimilarity(r1.getCategory(), r2.getCategory()));
							
							e.setSpaceDist(spaceDist);
							e.setTimeDist((int) TimeUnit.MILLISECONDS.toDays(timeDist));
							e.setSameCategory(sameCategory);
							
							edges.add(e);
						}
					}
				}
			}
			return edges;
		}
	}

	private void loadFromReportIDs(List<Integer> reportIDs)
			throws InterruptedException, ExecutionException {
		System.out.println("Loading graph nodes...");
		long start = System.currentTimeMillis();

		reportsStartTime = Long.MAX_VALUE;
		reportsEndTime = Long.MIN_VALUE;
		final Object timeStampsLock = new Object();

		graph.clear();

		// ########################
		final Object[] keys = (reportIDs != null) ? reportIDs.toArray()
				: reportsCache.getKeys().toArray();

		int processors = Runtime.getRuntime().availableProcessors();
		ExecutorService pool = Executors.newFixedThreadPool(processors);
		List<Future<Integer>> futures = new ArrayList<>();

		int lowerBound = 0;
		int upperBound = 0;
		int step = keys.length / processors;
		for (int i = 0; i < processors; i++) {
			lowerBound = upperBound;
			upperBound = (i == processors - 1) ? keys.length
					: (upperBound + step);
			final int finalLowerBound = lowerBound;
			final int finalUpperBound = upperBound;
			Future<Integer> f = pool.submit(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					int count = 0;
					for (int j = finalLowerBound; j < finalUpperBound; j++) {
						Report r = (Report) reportsCache.get(keys[j]).getObjectValue();
						int id = r.getID();
						long creationTime = r.getCreationTime();
						Node n = new Node(id);
						if (!graph.addNode(n))
							throw new IllegalStateException(
									"Failed to insert node (ID was in use)");
						synchronized (timeStampsLock) {
							if (creationTime < reportsStartTime)
								reportsStartTime = creationTime;
							if (creationTime > reportsEndTime)
								reportsEndTime = creationTime;
						}
						count++;
					}
					return count;
				}
			});
			futures.add(f);
		}
		int totalFromThreads = 0;
		for (Future<Integer> f : futures) {
			totalFromThreads += f.get();
		}
		pool.shutdown();
		int keysSize = reportsCache.getKeys().size();
		if (totalFromThreads != keys.length
				|| totalFromThreads != graph.getNodeCount()
				|| totalFromThreads != keysSize) {
			throw new IllegalStateException("totalFromThreads: "
					+ totalFromThreads + ", keys: " + keys.length
					+ ", nodes: " + graph.getNodeCount() + ", reports: "
					+ keysSize);
		}
		// ########################
		long end = System.currentTimeMillis();
		System.out.println("Finished loading graph nodes after "
				+ (end - start) + "ms");
		// Compute number of buckets
		if (reportsEndTime < reportsStartTime
				&& (reportsEndTime != Long.MIN_VALUE && reportsStartTime != Long.MAX_VALUE))
			throw new IllegalStateException(
					"Ending time of the reports cannot be previous to the starting time.");

		reportsTimeSpan = reportsEndTime - reportsStartTime;
		long maxDayDistInMs = TimeUnit.DAYS.toMillis(maxDayDist);
		int monthsSpan = (reportsTimeSpan % maxDayDistInMs == 0) ? (int) (reportsTimeSpan / maxDayDistInMs)
				: (int) (reportsTimeSpan / maxDayDistInMs) + 1;
		buckets = new Hashtable<Integer, List<Integer>>(monthsSpan);
		System.out.println("#buckets = " + buckets.size());
		transferDataToDB(monthsSpan);
	}

	private void transferDataToDB(int span)
			throws InterruptedException, ExecutionException {
		System.out.println("Transfering nodes to buckets...");

		int processors = Runtime.getRuntime().availableProcessors();
		ExecutorService pool = Executors.newFixedThreadPool(processors);
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

		final Object[] bucketLocks = new Object[span];
		for (int i = 0; i < bucketLocks.length; i++) {
			bucketLocks[i] = new Object();
		}

		// First: construct for each bucket a list of reportIDs from the reports
		// that correspond to said bucket.
		int lowerBound = 0;
		int upperBound = 0;
		final Object[] reportIDs = graph.getNodeIDs().toArray();
		int step = graph.getNodeCount() / processors;
		for (int i = 0; i < processors; i++) {
			lowerBound = upperBound;
			upperBound = (i == processors - 1) ? graph.getNodeCount()
					: (upperBound + step);
			final int finalLowerBound = lowerBound;
			final int finalUpperBound = upperBound;
			Future<Integer> f = pool.submit(new Callable<Integer>() {

				public Integer call() throws Exception {
					int count = 0;
					for (int j = finalLowerBound; j < finalUpperBound; j++) {
						Report rep = (Report) reportsCache.get(reportIDs[j]).getObjectValue();
						int bucketIndex = getBucketIndex(rep);
						synchronized (bucketLocks[bucketIndex]) {
							List<Integer> bucketReportIDs = buckets.get(bucketIndex);
							if (bucketReportIDs == null) {
								bucketReportIDs = new ArrayList<Integer>();
							}
							bucketReportIDs.add(rep.getID());
							buckets.put(bucketIndex, bucketReportIDs);
						}
						count++;
					}
					return count;
				}

				private int getBucketIndex(Report rep) {
					return (int) (Math.abs(rep.getCreationTime() - reportsStartTime) / TimeUnit.DAYS.toMillis(maxDayDist));
				}
			});
			futures.add(f);
		}

		int totalFromThreads = 0;
		for (Future<Integer> f : futures) {
			totalFromThreads += f.get();
		}
		
		int totalInBuckets = 0;
		for(Entry<Integer, List<Integer>> entry : buckets.entrySet()) {
			totalInBuckets += entry.getValue().size();
		}
		
		int totalKeys = reportsCache.getKeys().size();
		if (totalFromThreads != totalKeys
				|| totalFromThreads != graph.getNodeCount()
				|| totalInBuckets != totalKeys) {
			cleanupBuckets();
			throw new IllegalStateException("total: " + totalFromThreads
					+ ", nodes: " + graph.getNodeCount() + ", reports: " + totalKeys
					+ ", in buckets: " + totalInBuckets);
		}
	}

	private void cleanupBuckets() {
		buckets.clear();
	}

	private Cache getNewCache(String cacheName, long maxBytesLocalHeap,
			long maxBytesLocalOffHeap, boolean persistent, Hashtable<String, String> searchableAttributes) {
		if (databaseManager.cacheExists(cacheName)) {
			throw new IllegalArgumentException("Cache '" + cacheName
					+ "' already exists");
		}

		CacheConfiguration cacheConfig = new CacheConfiguration(cacheName, 0)
				.eternal(true);
		cacheConfig.setMaxBytesLocalHeap(maxBytesLocalHeap);
		
		// Overflow to OffHeap
		cacheConfig.overflowToOffHeap(true);
		if (maxBytesLocalOffHeap >= 1024 * 1024)
			cacheConfig.setMaxBytesLocalOffHeap(maxBytesLocalOffHeap);
		else
			cacheConfig.setMaxBytesLocalOffHeap("50M");

		// Overflow to Disk
		cacheConfig.setMaxEntriesLocalDisk(0);
		cacheConfig.persistence(new PersistenceConfiguration()
		.strategy(PersistenceConfiguration.Strategy.LOCALRESTARTABLE));
//		if(persistent){
//			cacheConfig.persistence(new PersistenceConfiguration()
//			.strategy(PersistenceConfiguration.Strategy.LOCALRESTARTABLE));
//		} else {
//			cacheConfig.persistence(new PersistenceConfiguration()
//			.strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP));
//		}
		
		cacheConfig.setMemoryStoreEvictionPolicy("LRU");

		// Reference the object directly
		cacheConfig.setCopyOnRead(false);

		Searchable searchable = new Searchable();
		if(searchableAttributes != null) {
			for(Entry<String, String> entry : searchableAttributes.entrySet())
				searchable.addSearchAttribute(new SearchAttribute().name(entry.getKey()).expression("value." + entry.getValue()));
		}
		
		cacheConfig.addSearchable(searchable);

		Cache newCache = new Cache(cacheConfig);
		databaseManager.addCache(newCache);
		
		return newCache;
	}

	private long getMaxAvailableHeapMemory() {
		Runtime rt = Runtime.getRuntime();
		return rt.maxMemory() - (rt.totalMemory() - rt.freeMemory());
	}

	private void checkForConsistency() {
		for (Object key : reportsCache.getKeys()) {
			// Report r = (Report) reportsCache.get(key).getObjectValue();
			if (!graph.containsNodeID((int) key)) {
				throw new IllegalStateException(
						"Cache "
								+ reportsCache.getName()
								+ " is not consistent with the data stored in the current graph.");
			}
		}
	}

	/**
	 * Returns the current graph.
	 * @return the current graph.
	 */
	public Graph getGraph() {
		return graph;
	}

	/**
	 * 
	 * @return Get the value of the maximum space distance (in meters) two
	 *         reports can be separated from one another in order to be
	 *         ST-Connected
	 */
	public double getMaxSpaceDist() {
		return maxSpaceDist;
	}

	/**
	 * @return Get the value of the maximum time distance (in milliseconds) two
	 *         reports can be separated from one another in order to be
	 *         ST-Connected.
	 */
	public long getMaxTimeDist() {
		return maxDayDist;
	}

	/**
	 * Returns the current maximal temporal distance in days.
	 * @return the current maximal temporal distance in days.
	 */
	public int getMaxDayDist() {
		return maxDayDist;
	}

	/**
	 * Sets a new value for the maximal spatial distance in meters.
	 * @param maxSpaceDist the new value for the maximal spatial distance in meters.
	 */
	public void setMaxSpaceDist(int maxSpaceDist) {
		this.maxSpaceDist = maxSpaceDist;
	}

	/**
	 * Sets a new value for the maximal temporal distance in days.
	 * @param maxDayDist the new value for the maximal temporal distance in days.
	 */
	public void setMaxDayDist(int maxDayDist) {
		this.maxDayDist = maxDayDist;
	}

	/**
	 * Returns the current CacheManager.
	 * @return the current CacheManager.
	 */
	public CacheManager getDatabaseManager() {
		return databaseManager;
	}

}
