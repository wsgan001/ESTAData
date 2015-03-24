package de.estadata.mining.graphmodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class represents a 'filtered view' of a certain graph by storing a subset of its node and edge IDs.
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 *
 */
public class GraphView {
	private List<Integer> nodeIDs;
	private List<Integer> edgeIDs;
	private Graph graph;
	
	/**
	 * Constructs a view associated with a specific graph.
	 * @param graph
	 */
	public GraphView(Graph graph) {
		nodeIDs = new ArrayList<>();
		edgeIDs = new ArrayList<>();
		this.graph = graph;
	}
	
	/**
	 * Adds a node ID to the view, as long as the ID corresponds to a node in the associated graph.
	 * @param id the node ID to be added to the view
	 */
	public void addNodeID(int id) {
		if(graph.containsNodeID(id))
			nodeIDs.add(id);
	}
	
	/**
	 * Add a collection of node IDs to the view. However, IDs that cannot be associated with any node in the graph are discarded.
	 * @param nodeIDs The collection of node IDs to be added to the view.
	 */
	public void addAllNodeIDs(Collection<Integer> nodeIDs) {
		for(int id : nodeIDs) {
			if(graph.containsNodeID(id))
				this.nodeIDs.add(id);
		}
	}
	
	/**
	 * Tests whether a certain node ID is associated with the view.
	 * @param id the ID 
	 * @return true, if the view contains said node ID, false otherwise.
	 */
	public boolean containsNodeID(int id) {
		return nodeIDs.contains(id);
	}
	
	/**
	 * Adds a edge ID to the view, as long as the ID corresponds to an edge in the associated graph.
	 * @param id the edge ID to be added to the view
	 */
	public void addEdgeID(int id) {
		edgeIDs.add(id);
	}
	
	/**
	 * Add a collection of edge IDs to the view. However, IDs that cannot be associated with any edge in the graph are discarded.
	 * @param nodeIDs The collection of edge IDs to be added to the view.
	 */
	public void addAllEdgeIDs(Collection<Integer> edgeIDs) {
		for(int id : edgeIDs) {
			this.edgeIDs.add(id);
		}
	}
	
	/**
	 * Tests whether a certain edge ID is associated with the view.
	 * @param id the ID 
	 * @return true, if the view contains said edge ID, false otherwise.
	 */
	public boolean containsEdgeID(int id) {
		return edgeIDs.contains(id);
	}
	
	/**
	 * Returns a list containing all node IDs associated with the view. 
	 * @return a list containing all node IDs associated with the view.
	 */
	public List<Integer> getNodeIDs() {
		return nodeIDs;
	}
	
	/**
	 * Returns a list containing all edge IDs associated with the view.
	 * @return a list containing all edge IDs associated with the view.
	 */
	public List<Integer> getEdgeIDs() {
		return edgeIDs;
	}
}
