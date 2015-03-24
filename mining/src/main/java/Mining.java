import java.io.File;
import java.util.Random;

import de.estadata.mining.datatransformation.BigMemory;
import de.estadata.mining.graphmodel.Graph;
import de.estadata.mining.modularityoptimizer.ModularityOptimizer.Algorithm;
import de.estadata.mining.modularityoptimizer.ModularityOptimizer.ModularityFunction;
import de.estadata.mining.stclustering.GraphClustering;
import de.estadata.mining.stclustering.STFiltering;
import de.estadata.mining.util.DataLoader;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Mining {

	public static void main(String[] args) {

		Namespace ns = null;
		try {
			ns = parseArguments(args);
		} catch (ArgumentParserException e1) {
			e1.printStackTrace();
			return;
		}
		String pointsCacheName = null;
		String clustersCacheName;
		
		Cache reportsCache;
		Cache clustersCache;

		String dataPath;
		String dataType;
		int meters;
		int days;
		double ratio;
		String configFilePath;
		CacheManager databaseManager = null;
		
		int exitStatus = 0;

		try {
			if ((configFilePath = ns.getString("config")) != null) {
				configFilePath = configFilePath.replace("[", "").replace("]",
						"");
				databaseManager = BigMemory.memoryDB(new File(configFilePath));
			} else {
				databaseManager = BigMemory.memoryDB();
			}

			pointsCacheName = ns.getString("reportscache");
			reportsCache = databaseManager.getCache(pointsCacheName);
			if(reportsCache == null) {
				throw new IllegalStateException("Failed to load cache named '" + pointsCacheName + "'");
			}
				
			clustersCacheName = ns.getString("clusterscache");
			clustersCache = databaseManager.getCache(clustersCacheName);
			if(clustersCache == null) {
				throw new IllegalStateException("Failed to load cache named '" + clustersCacheName + "'");
			}
			
			if ((dataPath = ns.getString("load")) != null) {
				dataPath = dataPath.replace("[", "").replace("]", "");
				dataType = ns.getString("type").replace("[", "").replace("]", "");
				ratio = Double.parseDouble(ns.getString("ratio").replace("[", "").replace("]", ""));
				if(ratio < 0 || ratio > 1)
					throw new IllegalArgumentException("Invalid ratio: " + ratio + ", should be a value between 0 and 1.");
				DataLoader.load(dataType, dataPath, reportsCache, ratio);
			}
			String mode = ns.getString("mode");
			if (mode == null) {
				return;
			} else {
				mode = mode.replace("[", "").replace("]", "");
			}
			
			if (mode.equals("filter")) {
				System.out.println("Filtering");
				String metersAsString;
				String daysAsString;
				if ((metersAsString = ns.getString("meters")) != null
						&& (daysAsString = ns.getString("days")) != null) {
					metersAsString = metersAsString.replace("[", "").replace(
							"]", "");
					daysAsString = daysAsString.replace("[", "").replace("]",
							"");
					meters = Integer.parseInt(metersAsString);
					days = Integer.parseInt(daysAsString);
					long start = System.currentTimeMillis();
					STFiltering filtering = new STFiltering(databaseManager,
							reportsCache, meters, days);
					filtering.generateAndTransferClusters(clustersCache);
					long end = System.currentTimeMillis();
					long time = end - start;
					System.out.println("First step took: " + time + " ms");
				} else if (ns.getString("meters") != null ^ ns.getString("days") != null) {
					throw new Exception(
							"Both --meters and --days must be provided");
				} else {
					throw new Exception(
							"No values for --meters or --days were provided");
				}
			} 
			else if (mode.equals("cluster")) {
				STFiltering filtering = new STFiltering(databaseManager, reportsCache, true);
				Graph graph = filtering.getGraph();
				
				System.out.println("Loaded graph has " + graph.getNodeCount() + " nodes and " + graph.getEdgeCount() + " edges");
				
				GraphClustering clustering = new GraphClustering(graph, reportsCache, clustersCache);
				
				String algorithmName = ns.getString("algorithm");
				if(algorithmName == null) {
					throw new IllegalStateException("Missing name of clustering algorithm to be executed");
				} else {
					algorithmName = algorithmName.toLowerCase().replace("[", "").replace("]", "");
				}
				
				if(algorithmName.equals("scan")) {
					int mu = Integer.parseInt(ns.getString("mu"));
					double epsilon = Double.parseDouble(ns.getString("eps"));
					
					long start = System.currentTimeMillis();
					clustering.runSCAN(epsilon, mu);
					clustering.generateAndTransferClusters();
					long end = System.currentTimeMillis();
					long time = end - start;
					System.out.println("Clustering with SCAN took: " + time + " ms");
					
				} else if(algorithmName.equals("louvain") || algorithmName.equals("louvain_mlv") || algorithmName.equals("slm")) {
					
					ModularityFunction modFunc;
					double resolution;
					Algorithm algorithm;
					int randomStarts;
					int iterations;
					long randomSeed;
					
					String modFuncAsString 		= ns.getString("modularity_function").replace("[", "").replace("]", "");
					String resolutionAsString 	= ns.getString("resolution").replace("[", "").replace("]", "");
					String randomStartsAsString = ns.getString("random_starts").replace("[", "").replace("]", "");
					String iterationsAsString 	= ns.getString("iterations").replace("[", "").replace("]", "");
					String randomSeedAsString 	= ns.getString("random_seed").replace("[", "").replace("]", "");
					
					if(modFuncAsString != null) {
						modFuncAsString = modFuncAsString.toLowerCase();
						if(modFuncAsString.equals("standard")) {
							modFunc = ModularityFunction.STANDARD;
						} else if(modFuncAsString.equals("alternative")) {
							modFunc = ModularityFunction.ALTERNATIVE;
						} else {
							throw new IllegalArgumentException("Invalid value for --modularity_function. Must be one of [standard|alternative]");
						}
					} else {
						throw new IllegalStateException("Failed to find value for --modularity_function. Must be one of [standard|alternative]");
					}
					
					if(resolutionAsString != null) {
						resolution = Double.parseDouble(resolutionAsString);
					} else {
						throw new IllegalStateException("Failed to find value for --resolution");
					}
					
					if(algorithmName.equals("louvain")) {
						algorithm = Algorithm.LOUVAIN;
					} else if(algorithmName.equals("louvain_mlv")) {
						algorithm = Algorithm.LOUVAIN_WITH_MULTILEVEL;
					} else {
						algorithm = Algorithm.SLM;
					}
					
					if(randomStartsAsString != null) {
						randomStarts = Integer.parseInt(randomStartsAsString);
					} else {
						throw new IllegalStateException("Failed to find value for --random_starts");
					}
					
					if(iterationsAsString != null) {
						iterations = Integer.parseInt(iterationsAsString);
					} else {
						throw new IllegalStateException("Failed to find value for --iterations");
					}
					
					if(randomSeedAsString != null) {
						randomSeed = Long.parseLong(randomSeedAsString);
					} else {
						Random rand = new Random();
						randomSeed = rand.nextLong();
					}
					
					long start = System.currentTimeMillis();
					clustering.runModularityOptimizer(modFunc, resolution, algorithm, randomStarts, iterations, randomSeed);
					clustering.generateAndTransferClusters();
					long end = System.currentTimeMillis();
					long time = end - start;
					System.out.println("Clustering step took: " + time + " ms");
					
				} else {
					throw new IllegalArgumentException("Invalid algorithm name: " + algorithmName + ". Must be one of (scan | louvain | louvain_mlv | slm)");
				}
			} else if(mode.equals("clean")) {
				databaseManager.removeAllCaches();
			}

		} catch (Exception e) {
			e.printStackTrace();
			exitStatus = 1;
		} finally {
			if (databaseManager != null) {
				System.out.println("Caches in database:");
				for(String cache : databaseManager.getCacheNames())
					System.out.println("\t" + cache);
				
				databaseManager.shutdown();
				System.exit(exitStatus);
			}
		}
	}

	private static Namespace parseArguments(String[] args)
			throws ArgumentParserException {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("estaMining")
				.defaultHelp(true).description(help);
		
		// load arguments
		parser.addArgument("-l", "--load").nargs(1)
				.help("load the data specified by a path");
		parser.addArgument("-t", "--type").choices("csv", "json")
				.setDefault("csv").help("the type of file to be loaded");
		parser.addArgument("-r", "--ratio").nargs(1).help("ratio of the amount of data to be loaded").setDefault("1");

		parser.addArgument("--mode").nargs(1).choices("filter", "cluster", "clean");

		// filtering arguments
		parser.addArgument("-m", "--meters").nargs(1)
				.help("distance in meters");
		parser.addArgument("-d", "--days").nargs(1).help("distance in days");
		parser.addArgument("-cf", "--config").nargs(1).help("config file");

		// clustering arguments
		//clustering algorithm
		parser.addArgument("--algorithm").nargs(1).choices("scan", "louvain", "louvain_mlv", "slm").help("the clustering algorithm to be run (scan | louvain | louvain_mlv | slm)." +
				"If scan is chosen, --mu and --eps can be set. See the specific help for each argument or visit http://www.ualr.edu/xwxu/publications/kdd07.pdf for further information.\n" +
				"If (louvain|louvain_mlv|slm) is chosen, --modularity, --modularity_function, --resolution, --random_starts, --iterations and --random_seed can be set. " +
				"See the help for each argument or visit http://www.ludowaltman.nl/slm/ for further information");
		
		//SCAN
		parser.addArgument("--mu").nargs(1).setDefault("2").help("an integer > 0, default: 2");
		parser.addArgument("--eps").nargs(1).setDefault("0.7").help("value in range [0, 1], default: 0.7");
		
		//Modularity
		parser.addArgument("--modularity_function").nargs(1).choices("standard", "alternative").setDefault("standard").help("the modularity function to be used (standard | alternative), default: standard");
		parser.addArgument("--resolution").nargs(1).setDefault("1.0").help("the value of the resolution parameter, default: 1.0");
		parser.addArgument("--random_starts").nargs(1).setDefault("10").help("the number of randoms starts executed by the modularity-base clustering algorithm, default: 10");
		parser.addArgument("--iterations").nargs(1).setDefault("10").help("the number of iterations per random start, default: 10");
		parser.addArgument("--random_seed").nargs(1).help("seed of the random number generator, default: random").setDefault("" + new Random().nextLong());

		// general arguments
		parser.addArgument("-rc", "--reportscache")
				.nargs(1)
				.setDefault("pointsCache")
				.help("the name of the cache for the reports (default: 'pointsCache')");
		parser.addArgument("-cc", "--clusterscache")
				.nargs(1)
				.setDefault("clustersCache")
				.help("the name of the cache for the report clusters (default: 'clustersCache')");

		return parser.parseArgs(args);
	}
	
	private static final String help = "This document provides a quick overview of the functionality of the .jar executable mining.jar, which\n" +
			"in turn provides an implementation of the framework proposed in [1]\n" +

			"The jar executable mining.jar allows the analysis of datasets in form of .csv and .json files while\n" +
			"using terracotta to store the information read from these datasets. Because the execution of a jar\n" +
			"file is influenced by the parametrization of the JVM running it, we assume in the following that\n" +
			"the program is run using the following command line arguments:\n" +
				
			"	java -d64 -Xmx(...) -XX:MaxDirectMemorySize=(...) -jar mining.jar -cf CONFIG_PATH ARGS -rc REP_CACHE -cc CLUSTERS_CACHE\n" +
				
			"In the above command, the '(...)' represent the corresponding values for the given parameters.\n" +
			"It is recommended that this last value be set as high as possible, since it influences greatly the\n" + 
			"overall performance of the analysis. Furthermore, the value passed to -Xmx should also be set to be \n" +
			"high, but not as high as -XX:MaxDirectMemorySize. Moreover, CONFIG_PATH is the path to the .xml file\n" +
			"with the terracotta configuration. In it, at least two caches should be configured: REP_CACHE and \n" +
			"CLUSTERS_CACHE. The first one is the cache that will contain all the reports loaded from csv/json files.\n" +
			"The second one will contain the resulting clusterings from the analysis. In the following examples, we\n" +
			"assume that these arguments are passed.\n" +
			
			"In order to analyse a given dataset, it must first be loaded and 'pre-processed'. This is done using\n" +
			"the following command:\n" +
			"	mining.jar -l PATH -t TYPE\n" + 
			"In this case, PATH is the (absolute or relative) path to the file/folder containing the file(s). TYPE\n" +
			"is the type of file containing the data. Thus, if PATH points to a directory, and TYPE=csv, the\n" +
			"program will read all data in every .csv file contained in said directory.\n" +
			
			"Once the data has been loaded, we can proceed to analyze it using the following arguments:\n" +
			"	mining.jar --mode filter -d DAYS -m METERS \n" +
			
			"The argument both values must be positive integers. For further information about appropiate values,\n" +
			"refer to [1]. Executing this results in the reports stored in REP_CACHE receiving a clusterID, and\n" +
			"in CLUSTERS_CACHE containing the generated Clusters.\n" +
			
			"Finally, we can also execute the second step of the framework:\n" +
			"	mining.jar --mode cluster --algorithm ALG [ARGS]\n" +
				
			"In this case, ALG is the algorithm to be used for clustering, and ARGS are the arguments for the chosen algorithm.\n" +
			"-----------------------------------------------------------\n" +
			"[1] Budde, M., Borges, J. D. M., Tomov, S., Riedel, T., & Beigl, M. Improving\n" + 
			"Participatory Urban Infrastructure Monitoring through Spatio-Temporal Analytics.\n" +
			"-----------------------------------------------------------\n";

}
