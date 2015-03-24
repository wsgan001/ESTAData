package de.estadata.mining.modularityoptimizer;

/**
 * This is an adaptation of the code provided in from http://www.ludowaltman.nl/slm, extended to read the necessary graph information from 
 * a {@link Graph} instance. The provided algorithms (SLM, Louvain and Louvain with multilevel refinement) remain unchanged. For further information
 * please visit the above mentioned website.
 *
 * @author Ludo Waltman
 * @author Nees Jan van Eck
 * @author Nicolas Loza
 */
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import de.estadata.mining.graphmodel.Graph;
import de.estadata.mining.graphmodel.Node;
import de.estadata.mining.util.MiningTools;

public class ModularityOptimizer {
	
	/**
	 * Available modularity-based algorithms: Louvain, Louvain with multilevel and SLM.
	 * @author Nicolas Loza
	 *
	 */
	public enum Algorithm {
        LOUVAIN, LOUVAIN_WITH_MULTILEVEL, SLM;
	};
	
	/**
	 * Available modularity functions: standard and alternative.
	 * @author Nicolas Loza
	 *
	 */
	public enum ModularityFunction {
		STANDARD, ALTERNATIVE
	};
	
	private Map<Integer, Integer> idMap;
	private List<Integer> nodeIDs;
	private Graph graph;
	
	private int nNodes;
    private int[] firstNeighborIndex;
    private int[] neighbor;
    private double[] edgeWeight2;
    private double[] nodeWeight;
	
    /**
     * Constructs an instance from a {@link Graph}. 
     * @param graph the underlying graph for the analysis.
     */
    public ModularityOptimizer(Graph graph) {
    	this.graph = graph;
    	
    	nodeIDs = MiningTools.asSortedList(graph.getNodeIDs());
    	if(nodeIDs == null || nodeIDs.size() == 0)
    		throw new IllegalStateException("nodeIDs (" + nodeIDs + ") is empty");
    	constructMap(nodeIDs);
    	
    	System.out.println("Initializing Modularity Optimizer...");
        
        precomputeNetworkDependencies(graph, nodeIDs);
    	
        System.out.println("Network precomputing finished");
    }
	
    /**
     * Runs a specified algorithm with specific parameters. For full specifications visit http://www.ludowaltman.nl/slm/.
     * @param modFunc The modularity function (see {@link ModularityFunction}).
     * @param resolution determines the granularity level at which communities are detected. 
     * 	Use a value of 1.0 for standard modularity-based community detection. 
     * Use a value above (below) 1.0 if you want to obtain a larger (smaller) number of communities.
     * @param algorithm the algorithm to be run (see {@link Algorithm}).
     * @param randomStarts the number of random starts
     * @param iterations the number of iterations per random start.
     * @param randomSeed the seed for the RNG.
     */
    public void run(ModularityFunction modFunc, double resolution, Algorithm algorithm, int randomStarts, int iterations, long randomSeed) {
        
        double modularity, maxModularity, resolution2;
        int i, j, nClusters;
        int[] cluster;
        long beginTime, endTime;
        Network network;
        Random random;

        System.out.println("Running Modularity Optimizer...");
        
        System.out.println("graph has " + graph.getNodeCount() + " nodes and " + graph.getEdgeCount() + " edges");
        
        if (modFunc == ModularityFunction.STANDARD) {    
            network = new Network(nNodes, firstNeighborIndex, neighbor, edgeWeight2, nodeWeight);
        } else {
        	network = new Network(nNodes, firstNeighborIndex, neighbor, edgeWeight2);
        }
        
        System.out.println("Generated network has " + network.getNNodes() + " nodes and " + network.getNEdges()/2 + " edges");

        System.out.println("Running " + ((algorithm == Algorithm.LOUVAIN) ? "Louvain algorithm" 
        		: ((algorithm == Algorithm.LOUVAIN_WITH_MULTILEVEL) ? "Louvain algorithm with multilevel refinement" 
        		: "smart local moving algorithm")) + "...");
        System.out.println();

        resolution2 = ((modFunc == ModularityFunction.STANDARD) ? (resolution / network.getTotalEdgeWeight()) : resolution);

        beginTime = System.currentTimeMillis();
        cluster = null;
        nClusters = -1;
        maxModularity = Double.NEGATIVE_INFINITY;
        random = new Random(randomSeed);
        for (i = 0; i < randomStarts; i++) {

            network.initSingletonClusters();

            j = 0;
            boolean update = true;
            do {

                if (algorithm == Algorithm.LOUVAIN)
                    update = network.runLouvainAlgorithm(resolution2, random);
                else if (algorithm == Algorithm.LOUVAIN_WITH_MULTILEVEL)
                    update = network.runLouvainAlgorithmWithMultilevelRefinement(resolution2, random);
                else if (algorithm == Algorithm.SLM)
                    network.runSmartLocalMovingAlgorithm(resolution2, random);
                j++;

                modularity = network.calcQualityFunction(resolution2);

            }
            while ((j < iterations) && update);

            if (modularity > maxModularity) {
                network.orderClustersByNNodes();
                cluster = network.getClusters();
                nClusters = network.getNClusters();
                maxModularity = modularity;
            }

        }
        endTime = System.currentTimeMillis();
        
        nClusters = adaptClusterIDs(cluster);
        
        System.out.format("Maximum modularity in %d random starts: %.4f%n", randomStarts, maxModularity);
        System.out.format("Number of communities: %d%n", nClusters);
        System.out.format("Elapsed time: %d seconds%n", Math.round((endTime - beginTime) / 1000.0));
        System.out.println();
        
        i = 0;
        for(Integer nodeID : nodeIDs) {
        	Node n = graph.getNode(nodeID);
        	n.setClusterID(cluster[i++]);
//        	MiningTools.setNodeLabel(n, STFiltering.clusterIDLabel, "" + cluster[i++]);
        }
    }
    
    private int adaptClusterIDs(int[] clusterIDs) {
    	Map<Integer, Integer> idsMap = new HashMap<>(clusterIDs.length);
    	for(int id : clusterIDs) {
    		if(idsMap.containsKey(id)) {
    			idsMap.put(id, idsMap.get(id) + 1);
    		} else {
    			idsMap.put(id, 1);
    		}
    	}
    	for(int i = 0; i < clusterIDs.length; i++) {
    		int id = clusterIDs[i];
    		if(idsMap.get(id) == 1)
    			clusterIDs[i] = -1;
    		else
    			clusterIDs[i] += 1; 
    	}
    	int communities = 0;
    	for(Entry<Integer, Integer> entry : idsMap.entrySet()) {
    		if(entry.getValue() > 1)
    			communities++;
    	}
    	return communities;
    }
    
    private void constructMap(List<Integer> nodeIDs) {
    	idMap = new HashMap<>(nodeIDs.size());
    	for(int i = 0; i < nodeIDs.size(); i++) {
    		int nodeID = nodeIDs.get(i);
    		idMap.put(nodeID, i);
    	}
    }
   
    private void precomputeNetworkDependencies(Graph graph, List<Integer> nodeIDs) {
        double[] edgeWeight1;
        int i, j, nEdges, nLines;
        int[] nNeighbors, node1, node2;
        
        nLines = graph.getEdgeCount();
        
        node1 = new int[nLines];
        node2 = new int[nLines];
        edgeWeight1 = new double[nLines];
        i = -1;
        j = 0;
        for (Integer nodeID : nodeIDs) {
       	  Node n = graph.getNode(nodeID);
    	  for(Integer neighborID : n.getNeighborIDs()) {
    		  if(neighborID > nodeID) {
    			  node1[j] = idMap.get(nodeID);
    		      if (node1[j] > i)
    		          i = node1[j];
    		      node2[j] = idMap.get(neighborID);
    		      if (node2[j] > i)
    		          i = node2[j];
    		      edgeWeight1[j++] = graph.getEdge(n.getConnectingEdgeID(neighborID)).getWeight();
    		  }
    	  }
        }
        nNodes = i + 1;

        nNeighbors = new int[nNodes];
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i])
            {
                nNeighbors[node1[i]]++;
                nNeighbors[node2[i]]++;
            }

        firstNeighborIndex = new int[nNodes + 1];
        nEdges = 0;
        for (i = 0; i < nNodes; i++)
        {
            firstNeighborIndex[i] = nEdges;
            nEdges += nNeighbors[i];
        }
        firstNeighborIndex[nNodes] = nEdges;

        neighbor = new int[nEdges];
        edgeWeight2 = new double[nEdges];
        Arrays.fill(nNeighbors, 0);
        for (i = 0; i < nLines; i++)
            if (node1[i] < node2[i])
            {
                j = firstNeighborIndex[node1[i]] + nNeighbors[node1[i]];
                neighbor[j] = node2[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node1[i]]++;
                j = firstNeighborIndex[node2[i]] + nNeighbors[node2[i]];
                neighbor[j] = node1[i];
                edgeWeight2[j] = edgeWeight1[i];
                nNeighbors[node2[i]]++;
            }

        nodeWeight = new double[nNodes];
        for (i = 0; i < nEdges; i++) {
            nodeWeight[neighbor[i]] += edgeWeight2[i];
        }
        

    }

	public Graph getGraph() {
		return graph;
	}

	public void setGraph(Graph graph) {
		this.graph = graph;
	}
}