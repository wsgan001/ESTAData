package de.estadata.mining.scan;

import de.estadata.mining.graphmodel.*;

import de.estadata.mining.util.MiningTools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 
 * This class implements the Structural Clustering Algorithm for Networks (SCAN) as presented by X. Xu et al. in their paper.
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 *
 */

public class SCAN {
	private Graph graph;
	private double epsilon;
	private int mu;
	private Map<Integer, List<Integer>> clustersMap;	//Key: clusterID; value: list of IDs of the nodes belonging to such cluster
	private List<Node> hubs;
	private List<Node> outliers;
	private List<Integer> clusterIDs;
	
	private final String unclassifiedLabel = "unclassified";
	private final String nonMemberLabel = "non-member";
	public final String outlierLabel = "outlier";
	public final String hubLabel = "hub";
	
	/**
	 * Constructs a SCAN instance for a graph with the specified parameters. For further information see 
	 * SCAN: A Structural Clustering Algorithm for Networks, by X. Xu et al.
	 * @param graph
	 * @param epsilon
	 * @param mu
	 */
	public SCAN(Graph graph, double epsilon, int mu) {
		System.out.println("Initializing SCAN...");
		if(epsilon < 0 || epsilon > 1) {
			throw new IllegalArgumentException("Invalid value for epsilon: " + epsilon + ", must be in the range [0,1]");
		}
		if(mu < 2) {
			throw new IllegalArgumentException("Invalid value for mu: " + mu + ", should be at least 2");
		}
		this.graph = graph;
		this.epsilon = epsilon;
		this.mu = mu;
		clustersMap = new HashMap<>();
		System.out.println("SCAN initalized.");
	}
	
	/**
	 * 
	 * @return The graph of this SCAN instance. If execute() was not executed, the graph remains the same as before.
	 */
	public Graph getGraph() {
		return graph;
	}

	/**
	 * Set a new graph for this SCAN instance
	 * @param graph
	 */
	public void setGraph(Graph graph) {
		this.graph = graph;
	}
	
	/**
	 * 
	 * @return The value set for the algorithm parameter epsilon
	 */
	public double getEpsilon() {
		return epsilon;
	}
	
	/**
	 * 
	 * @param epsilon A new value for the algorithm parameter epsilon
	 */
	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	/**
	 * 
	 * @return The value set for the algorithm parameter epsilon
	 */
	public int getMu() {
		return mu;
	}

	/**
	 * 
	 * @param mu A new value for the algorithm variable epsilon
	 */
	public void setMu(int mu) {
		this.mu = mu;
	}
	
	/**
	 * 
	 * @return 
	 *  If execute() was not called: null.<pre></pre>
	 *  If execute was called: The set of disjoint clusters found on the current graph.<pre></pre>
	 *  Note: if execute() and afterwards setGraph() were executed, this method still returns the clusters found on the previous graph until execute() is called again.
	 */
	public Map<Integer, List<Integer>> getClustersMap() {
		return clustersMap;
	}
	
	/**
	 * 
	 * @return 
	 * If execute() was not called: null. <pre></pre>
	 * If execute() was called: the set of hubs found on the current graph.<pre></pre>
	 * Node: if execute() and afterwards setGraph() were executed, this method still returns the outliers found on the previous graph until execute() is called again.
	 */
	public List<Node> getHubs() {
		return hubs;
	}
	
	/**
	 * 
	 * @return
	 * If execute() was not called: null. <pre></pre>
	 * If execute() was called: the set of outliers found on the current graph.<pre></pre>
	 * Node: if execute() and afterwards setGraph() were executed, this method still returns the outliers found on the previous graph until execute() is called again.
	 */
	public List<Node> getOutliers() {
		return outliers;
	}
	
	/**
	 * 
	 * @return The name of the label under which each node stores the information whether it has been marked as an outlier ("TRUE") or not ("FALSE")
	 */
	public String getOutlierLabel() {
		return outlierLabel;
	}
	
	/**
	 * 
	 * @return The name of the label under which each node stores the information whether it has been marked as a hub ("TRUE") or not ("FALSE")
	 */
	public String getHubLabel() {
		return hubLabel;
	}

	/**
	 * Executes the SCAN algorithm on the specified graph with the specified parameters epsilon and mu. The results are stored in the same graph
	 * @param startFrom the starting cluster ID
	 * @return the highest cluster ID
	 */
	public int run(int startFrom) {
		System.out.println("Starting SCAN...");
		
		hubs = new LinkedList<Node>();
		outliers = new LinkedList<Node>();
		
		System.out.println("Starting inititial classification of SCAN nodes...");
		for(int id : graph.getNodeIDs()) {
			Node v = graph.getNode(id);
			MiningTools.setNodeLabel(v, unclassifiedLabel, "TRUE");
			MiningTools.setNodeLabel(v, nonMemberLabel, "FALSE");
			MiningTools.setNodeLabel(v, hubLabel, "FALSE");
			MiningTools.setNodeLabel(v, outlierLabel, "FALSE");
			v.setClusterID(Integer.MIN_VALUE);
		}
		
		System.out.println("Growing clusters...");
		LinkedList<Node> nonMemberNodes = new LinkedList<Node>();
		clusterIDs = new ArrayList<Integer>();
		int currentClusterID = startFrom;
		for(int id : graph.getNodeIDs()) {
			Node v = graph.getNode(id);
			if(MiningTools.nodeIsLabeledAs(v, unclassifiedLabel)) {
				//Step 1. check whether v is a core;
				if(isCore(v)) {
					//Step 2.1. if v is a core, a new cluster is expanded;
					clusterIDs.add(currentClusterID);
					LinkedList<Node> queue = new LinkedList<Node>();
					queue.addAll(getEpsilonNeighborhood(v));
					while(!queue.isEmpty()) {
						Node y = queue.peek();
						if(isCore(y)) {
							List<Node> yENeigh = getEpsilonNeighborhood(y);
							for(Node x : yENeigh) {
								if(MiningTools.nodeIsLabeledAs(x, nonMemberLabel)) {
									MiningTools.setNodeLabel(x, nonMemberLabel, "FALSE");
//									MiningTools.setNodeLabel(x, clusterIdLabel, ""+currentClusterID);
									x.setClusterID(currentClusterID);
									assignToCluster(x, currentClusterID);
									nonMemberNodes.remove(x);
								}
								if(MiningTools.nodeIsLabeledAs(x, unclassifiedLabel)) {
									MiningTools.setNodeLabel(x, unclassifiedLabel, "FALSE");
//									MiningTools.setNodeLabel(x, clusterIdLabel, ""+currentClusterID);
									x.setClusterID(currentClusterID);
									assignToCluster(x, currentClusterID);
									if(!queue.contains(x)) {
										queue.add(x);
									}
								}
							}
						}
						queue.remove(y);
					}
					currentClusterID++;
				} else {
					//Step 2.2. if v is not a core, it is labeled as non-member
					MiningTools.setNodeLabel(v, nonMemberLabel, "TRUE");
					MiningTools.setNodeLabel(v, unclassifiedLabel, "FALSE");
					nonMemberNodes.add(v);
				}
			}
		}
		//Step 3. further classifies non-members
		for(Node v : nonMemberNodes) {
			List<Node> vStructure = getStructure(v);
			List<String> neighborClusterIDs = new ArrayList<>();
			boolean labeled = false;
			for(Node x : vStructure) {
//				String xCLusterID = MiningTools.getNodeLabelValue(x, clusterIdLabel);
				String xCLusterID = "" + x.getClusterID();
				if(!xCLusterID.equals("")) {
					if(neighborClusterIDs.isEmpty()) {
						neighborClusterIDs.add(xCLusterID);
					} else if(!neighborClusterIDs.contains(xCLusterID)){
						MiningTools.setNodeLabel(v, hubLabel, "TRUE");
//						MiningTools.setNodeLabel(v, clusterIdLabel, "-1");
						v.setClusterID(-1);
						assignToCluster(v, -1);
						hubs.add(v);
						labeled = true;
						break;
					}
				}
			}
			if(!labeled) {
				MiningTools.setNodeLabel(v, outlierLabel, "TRUE");
//				MiningTools.setNodeLabel(v, clusterIdLabel, "-1");
				v.setClusterID(-1);
				assignToCluster(v, -1);
				outliers.add(v);
			}
		}
		System.out.println("SCAN finished");
		return --currentClusterID;
	}
	
	private void assignToCluster(Node v, int clusterID) {
		if(clustersMap.containsKey(clusterID)) {
			clustersMap.get(clusterID).add(v.getID());
		} else {
			List<Integer> cluster = new ArrayList<>();
			cluster.add(v.getID());
			clustersMap.put(clusterID, cluster);
		}
	}

	private List<Node> getStructure(Node v) {
		List<Node> structure = new ArrayList<Node>();
		structure.add(v);
		
		for(int id : v.getNeighborIDs()) {
			structure.add(graph.getNode(id));
		}
		return structure;
	}
	
	private double structuralSimilarity(Node v, Node w) {
		List<Node> vStruct = getStructure(v);
		List<Node> wStruct = getStructure(w);
		
		double b = (double) (vStruct.size() * wStruct.size());
		b = Math.sqrt(b);
		
		//Intersection of the two lists is stored in the first one
		vStruct.retainAll(wStruct);
		
		//size of the intersection
		double a = (double) vStruct.size();
		
		a = a / b;
		
		return a;
	}
	
	private List<Node> getEpsilonNeighborhood(Node v) {
		List<Node> vStructure = getStructure(v);
		List<Node> eNeighborhood = new ArrayList<Node>();
		for(Node w : vStructure) {
			double structuralSimilarity = structuralSimilarity(v, w);
			if(structuralSimilarity >= this.epsilon) {
				eNeighborhood.add(w);
			}
		}
		return eNeighborhood;
	}
	
	private boolean isCore(Node v) {
		return getEpsilonNeighborhood(v).size() >= this.mu;
	}
}
