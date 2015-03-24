package de.estadata.mining.graphmodel;

import java.io.Serializable;

/**
 * This class represents an (undirected) edge between two nodes for the graph structure {@link Graph}. It always contains the following information:
 * edge ID, source node ID, target node ID and edge weight. However, it may also feature the two nodes' spatial distance, 
 * temporal distance and whether they share category. 
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 *
 */
public class Edge implements Serializable, Comparable<Edge>{
	
	private static final long serialVersionUID = 6364613447061151426L;
	private int id;
	private int sourceID;
	private int targetID;
	private double weight;
	
	private int spaceDist;
	private int timeDist;
	private boolean sameCategory;
	
	/**
	 * Constructs an edge between the two given nodes with ID -1 and weight 1.0.
	 * @param source Source node
	 * @param target Target node
	 */
	public Edge(Node source, Node target) {
		this(source, target, 1.0);
	}
	
	/**
	 * Constructs an edge between the two given nodes with a specific weight. Its ID is -1.
	 * @param source Source node
	 * @param target Target node
	 * @param weight Edge weight
	 */
	public Edge(Node source, Node target, double weight) {
		this(-1, source, target, weight);
	}
	
	/**
	 * Constructs an edge between the two given nodes with a specific ID. Its weight is 1.0.
	 * @param id
	 * @param source
	 * @param target
	 */
	public Edge(int id, Node source, Node target) {
		this(id, source, target, 1.0);
	}
	
	/**
	 * Constructs an edge between the two given nodes with specific ID and weight.
	 * @param id
	 * @param startNode
	 * @param endNode
	 * @param weight
	 */
	public Edge(int id, Node startNode, Node endNode, double weight) {
		this.id = id;
		this.sourceID = startNode.getID();
		this.targetID = endNode.getID();
		this.weight = weight;
	}
	
	/**
	 * Returns the ID of the edge's source node.
	 * @return the ID of the edge's source node.
	 */
	public int getSourceID() {
		return sourceID;
	}
	
	/**
	 * Returns the ID of the edge's target node.
	 * @return the edge's target node.
	 */
	public int getTargetID() {
		return targetID;
	}
	
	/**
	 * If the given parameter is an end of this edge, returns the other end.
	 * @param n one of this edge's ends.
	 * @return the ID of the edge's other end.
	 * @throws IllegalArgumentException If the given parameter is not an end of the edge.
	 */
	public int getOtherEndID(Node n) {
		if(n.getID() == sourceID)
			return targetID;
		else if(n.getID() == targetID)
			return sourceID;
		else
			throw new IllegalArgumentException("Cannot get other end of edge: given node is neither start nor end node.");
	}
	
	/**
	 * The edge's ID.
	 * @return the edge's ID.
	 */
	public int getID() {
		return id;
	}
	
	/**
	 * Sets the Edge's ID. Should only by accessed by the class Graph.
	 * @param id The new edge ID.
	 */
	protected void setID(int id) {
		this.id = id;
	}
	
	/**
	 * Returns the edge's weight.
	 * @return the edge's weight.
	 */
	public double getWeight() {
		return this.weight;
	}
	
	/**
	 * Get the lowest node ID of the two edge's ends.
	 * @return the lowest ID from the edge's ends.
	 */
	public int getLowestNodeID() {
		return (getSourceID() < getTargetID()) ? getSourceID() : getTargetID();
	}
	
	/**
	 * Get the highest node ID of the two edge's ends.
	 * @return the highest ID from the edge's ends.
	 */
	public int getHighestNodeID() {
		return (getSourceID() > getTargetID()) ? getSourceID() : getTargetID();
	}
	
	/**
	 * Sets a new value for the edge's weight.
	 * @param weight the new edge's weight.
	 */
	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	/**
	 * Compares the edge with another one for equality. 
	 * @param e The other edge.
	 * @return true, if both edges' ends match, false otherwise.
	 */
	public boolean equals(Edge e) {
		if(e.getSourceID() == sourceID) {
			return e.getTargetID() == targetID;
		} else if(e.getSourceID() == targetID) {
			return e.getTargetID() == sourceID;
		} else {
			return false;
		}
	}
	
	/**
	 * Tests if the edge points from one node to itself.
	 * @return true, if the source node ID and the target node ID match, false otherwise. 
	 */
	public boolean isSelfLoop() {
		return sourceID == targetID;
	}

	/**
	 * Returns a string of the form 'ID1 -> ID2', where ID1 is the source node ID, and ID2 is the target node ID.
	 */
	public String toString() {
		return getSourceID() + " -> " + getTargetID();
	}

	@Override
	public int compareTo(Edge e) { 
		int lowestID1 = getLowestNodeID();
		int lowestID2 = e.getLowestNodeID();
		if(lowestID1 < lowestID2)
			return -1;
		else if(lowestID1 == lowestID2)
			return 0;
		else
			return 1;
	}

	public int getSpaceDist() {
		return spaceDist;
	}

	public void setSpaceDist(int spaceDist) {
		this.spaceDist = spaceDist;
	}

	public int getTimeDist() {
		return timeDist;
	}

	public void setTimeDist(int timeDist) {
		this.timeDist = timeDist;
	}

	public boolean isSameCategory() {
		return sameCategory;
	}

	public void setSameCategory(boolean sameCategory) {
		this.sameCategory = sameCategory;
	}
	
}
