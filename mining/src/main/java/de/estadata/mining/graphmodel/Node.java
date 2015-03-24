package de.estadata.mining.graphmodel;

import java.io.Serializable;
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a node for the graph structure {@link Graph}.
 * 
 * @author Nicolas Loza
 *
 */
public class Node implements Serializable, Comparable<Node> {

	private static final long serialVersionUID = -5815874186229001009L;
	private Hashtable<String, String> attributes;
	private int id;
	private Hashtable<Integer, Integer> edges;			//key: adjacent edge ID; 	value: same as key
	private Hashtable<Integer, Integer> neighbors;		//key: neighbor ID; 		value: id of connecting edge
	private int clusterID;
	
	/**
	 * Constructs a node with a specific ID.
	 * @param id the ID of the node
	 */
	public Node(int id) {
		this.id = id;
		attributes = new Hashtable<String, String>();
		edges = new Hashtable<>();
		neighbors = new Hashtable<>();
		clusterID = Integer.MIN_VALUE;
	}
	
	/**
	 * Adds an attribute to the node.
	 * @param attribute the name of the attribute.
	 * @param value the value of the attribute. If null, the value previously associated with the attribute is removed. 
	 */
	public void setAttribute(String attribute, String value) {
		if(value == null) {
			attributes.remove(attribute);
		} else {
			attributes.put(attribute, value);
		}
	}
	
	/**
	 * Returns the value associated with the given attribute name.
	 * @param attribute the name of the attribute.
	 * @return The value associated with the attribute (null if non-existent).
	 */
	public String getAttribute(String attribute) {
		return attributes.get(attribute);
	}
	
	protected boolean addAdjacentEdge(Edge e) {
		int otherEndID = e.getOtherEndID(this);
		if(isNeighbor(otherEndID)) {
			return false;
		} else {
			edges.put(e.getID(), e.getID());
			neighbors.put(otherEndID, e.getID());
			return true;
		}
	}
	
	protected boolean removeAdjacentEdge(Edge e) {
		if(e == null || !edges.containsKey(e.getID())) {
			throw new IllegalArgumentException("Edge cannot be removed: is either null or is not an adjacent edge of this node.");
		}
		int otherEndID = e.getOtherEndID(this);
		neighbors.remove(otherEndID);
		return edges.remove(e.getID()) != null;
	}
	
	/**
	 * Returns a list containing all edges linked to the node.
	 * @return a list containing all edges linked to the node.
	 */
	public List<Integer> getAdjacentEdgesIDs() {
		List<Integer> edgesList = new LinkedList<>();
		edgesList.addAll(edges.values());
		return edgesList;
	}
	
	/**
	 * Returns a collection containing the IDs of all nodes linked to the current one through an edge. 
	 * @return the IDs of nodes linked to the current one.
	 */
	public Collection<Integer> getNeighborIDs() {
		return neighbors.keySet();
	}
	
	/**
	 * Returns the ID of the edge connecting the current node to the node associated to the given ID. 
	 * @param neighborID the ID of the node whose possible neighborhood is being examined.
	 * @return true, if the current node is linked to the one with the ID given as parameter, false otherwise.
	 */
	public Integer getConnectingEdgeID(int neighborID) {
		return neighbors.get(neighborID);
	}
	
	/**
	 * Tests whether the current node and another one are neighbors.
	 * @param n the other node
	 * @return true if the nodes are linked through an edge, false otherwise.
	 */
	public boolean isNeighbor(Node n) {
		return neighbors.containsKey(n.getID());
	}
	
	/**
	 * Tests whether the current node and another one (given by its ID) are neighbors.
	 * @param id the ID of the other node.
	 * @return true if the nodes are linked through an edge, false otherwise.
	 */
	public boolean isNeighbor(int id) {
		return neighbors.containsKey(id);
	}
	
	/**
	 * Tests whether two nodes are equal.
	 * @param n the other node
	 * @return true if both nodes' IDs are the same, false otherwise.
	 */
	public boolean equals(Node n) {
		return id == n.getID();
	}
	
	/**
	 * Returns the node's ID.
	 * @return the node's ID.
	 */
	public int getID() {
		return id;
	}
	
	public String toString() {
		return "" + id;
	}

	@Override
	public int compareTo(Node n) {
		if(this.getID() < n.getID())
			return -1;
		else if(this.getID() == n.getID())
			return 0;
		else
			return 1;
	}

	public Hashtable<String, String> getAttributes() {
		return attributes;
	}

	public int getClusterID() {
		return clusterID;
	}

	public void setClusterID(int clusterID) {
		this.clusterID = clusterID;
	}
}
