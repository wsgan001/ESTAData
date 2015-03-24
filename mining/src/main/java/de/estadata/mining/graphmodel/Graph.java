package de.estadata.mining.graphmodel;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.gephi.graph.api.*;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.io.exporter.plugin.ExporterGraphML;
import org.gephi.io.exporter.spi.CharacterExporter;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;
import org.gephi.utils.progress.ProgressTicketProvider;
import org.openide.util.Lookup;

import de.estadata.mining.gephiextension.ExporterGML;
import de.estadata.mining.gephiextension.ProgressTicketProviderImpl;
import de.estadata.mining.util.MiningTools;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * This class models an undirected graph using Terracotta Caches as underlying structure for the storage of {@link Edge} and
 * {@link Node} instances.
 * 
 * @author Nicolas Loza (nico.loza@gmail.com)
 *
 */
public class Graph {
	private Cache nodesCache;
	private Cache edgesCache;
	private TreeMap<Integer, Integer> nodeIDs;
	private TreeMap<Integer, Integer> edgeIDs;
	
	private UndirectedGraph gephiGraph;
	private GraphModel graphModel;
	private ProjectController pc;
	private Workspace workspace;
	private boolean gephiInitialized = false;
	
	
	@SuppressWarnings("unchecked")
	/**
	 * Constructs a graph using the information stored in the two given caches by searching for {@link Node} instances in the first one (nodesCache)
	 * and for {link Edge} instances in the second one (edgesCache). If none found, the graph is 'empty'.
	 * @param nodesCache
	 * @param edgesCache
	 */
	public Graph(Cache nodesCache, Cache edgesCache) {
		this.nodesCache = nodesCache;
		this.edgesCache = edgesCache;
		edgeIDs = new TreeMap<Integer, Integer>();
		nodeIDs = new TreeMap<Integer, Integer>();
		
		Map<Object, Node> map = MiningTools.getCacheObjectsAsMap(nodesCache, nodesCache.getKeys(), Node.class);
		for(Entry<Object, Node> entry : map.entrySet()) {
			nodeIDs.put((Integer) entry.getKey(), (Integer) entry.getKey());
		}
		
		Map<Object, Edge> map2 = MiningTools.getCacheObjectsAsMap(edgesCache, edgesCache.getKeys(), Edge.class);
		for(Entry<Object, Edge> entry : map2.entrySet()) {
			edgeIDs.put((Integer) entry.getKey(), (Integer) entry.getKey());
			Edge e = entry.getValue();
			getNode(e.getSourceID()).addAdjacentEdge(e);
			getNode(e.getTargetID()).addAdjacentEdge(e);
		}
	}

	/**
	 * Adds a node instance to the graph unless there is another node in the graph with the same node ID.
	 * @param n The node to be added.
	 * @return false if a node with the same ID is already present in the graph, true otherwise.
	 */
	public synchronized boolean addNode(Node n) {
		if(n == null)
			throw new IllegalArgumentException("Argument is null");
		
		Integer oldID = nodeIDs.put(n.getID(), n.getID());
		if(oldID != null) {
			return false;
		}
		nodesCache.put(new Element(n.getID(), n));
		return true;
	}
	
	/**
	 * Returns, if existent, the node with the given ID.
	 * @param id the id of the node to be returned
	 * @return a node instance, if the node ID is present in the graph, null otherwise.
	 */
	public Node getNode(int id) {
		if(!containsNodeID(id)) {
			return null;
		} else {
			Element e = nodesCache.get(id);
			if(e != null) {
				return (Node) e.getObjectValue();
			} else {
				System.err.println("Inconsistency between nodeIDs and nodes cache found");
				nodeIDs.remove(id);
				return null;
			}
		}	
	}
	
	/**
	 * Tests if there is a node in the graph with a certain ID.
	 * @param id to be searched for in the graph.
	 * @return true, if a node with the given ID is present in the graph, false otherwise.
	 */
	public boolean containsNodeID(int id) {
		return nodeIDs.containsKey(id);
	}
	
	/**
	 * Returns a list containing the neighbors of a given node, i.e. those linked to the given node by an edge.
	 * @param n the node whose neighborhood is to be returned.
	 * @return null, if the node is not present in the graph. Otherwise, a list of nodes that can be empty if the parameter node is isolated 
	 * (i.e. has no connections to other nodes).
	 */
	public List<Node> getNeighbors(Node n) {
		if(!containsNodeID(n.getID()))
			return null;
		List<Node> neighbors = new ArrayList<Node>();
		Map<Object, Element> neighborsMap = nodesCache.getAll(n.getNeighborIDs());
		for(Entry<Object, Element> entry : neighborsMap.entrySet()) {
			if(entry.getValue() != null) {
				neighbors.add((Node) entry.getValue().getObjectValue());
			}
		}
		return neighbors;
	}
	
	/**
	 * Adds an edge to the graph, as long as both of its ends are nodes already contained in the graph.
	 * @param e The edge to be added.
	 * @throws IllegalArgumentException If at least one of the edge's ends is not contained in the graph.
	 */
	public synchronized void addEdge(Edge e) {
		if(!containsNodeID(e.getSourceID())) {
			throw new IllegalArgumentException("Start node does not exist.");
		}
		
		if(!containsNodeID(e.getTargetID())) {
			throw new IllegalArgumentException("Target node does not exist.");
		}
		
		Node startNode = getNode(e.getSourceID());
		Node endNode = getNode(e.getTargetID());
		if(startNode.isNeighbor(endNode)) {
			if(endNode.isNeighbor(startNode)) {
				return;
			} else {
				throw new IllegalStateException("Neighborhood should be reflexive");
			}
		} else if(endNode.isNeighbor(startNode)) {
			throw new IllegalStateException("Neighborhood should be reflexive");
		}
		
		int id = generateEdgeID(); 
		e.setID(id);
		edgeIDs.put(id, id);
		
		startNode.addAdjacentEdge(e);
		endNode.addAdjacentEdge(e);
		
		edgesCache.put(new Element(e.getID(), e));
	}
	
	/**
	 * Returns, if existent, the edge with the given ID.
	 * @param id the ID of the searched edge.
	 * @return an Edge instance if there exists an Edge in the graph with the given ID, null otherwise.
	 */
	public Edge getEdge(int id) {
		Element e = edgesCache.get(id);
		if(e != null) {
			return (Edge) e.getObjectValue();
		} else {
			return null;
		}
	}
	
	/**
	 * Tests whether two nodes are neighbors in the graph (as long as both are contained in it).
	 * @param n1 The first node
	 * @param n2 The second node
	 * @return true if both nodes are contained in the graph and are linked through an edge, false otherwise.
	 */
	public boolean areNeighbors(Node n1, Node n2) {
		return containsNodeID(n1.getID()) && containsNodeID(n2.getID()) && n1.isNeighbor(n2);
	}
	
	/**
	 * Returns a set containing the IDs of all nodes currently present in the graph.
	 * @return a set containing the IDs of all nodes currently present in the graph.
	 */
	public Set<Integer> getNodeIDs() {
		return nodeIDs.keySet();
	}
	
	/**
	 * Returns a set containing the IDs of all edges currently present in the graph.
	 * @return a set containing the IDs of all edges currently present in the graph.
	 */
	public Set<Integer> getEdgeIDs() {
		return edgeIDs.keySet();
	}
	
	/**
	 * Returns the number of nodes currently in the graph.
	 * @return the number of nodes currently in the graph.
	 */
	public int getNodeCount() {
		return nodeIDs.size();
	}
	
	/**
	 * Returns the number of edges currently in the graph.
	 * @return the number of edges currently in the graph.
	 */
	public int getEdgeCount() {
		return edgeIDs.size();
	}
	
	/**
	 * Removes a given node from the graph, if present
	 * @param n the node to be removed from the graph.
	 * @return true, if the node was successfully removed, false otherwise.
	 */
	public boolean removeNode(Node n) {
		if(containsNodeID(n.getID())) {
			removeAllEdges(n);
			nodeIDs.remove(n.getID());
			return nodesCache.remove(n.getID());
		} else {
			return false;
		}
	}
	
	/**
	 * Removes a given edge from the graph, if present.
	 * @param e the edge to be removed from the graph.
	 * @return true, if the edge was successfully removed, false otherwise.
	 */
	public boolean removeEdge(Edge e) {
		Node startNode = getNode(e.getSourceID());
		Node endNode = getNode(e.getTargetID());
		startNode.removeAdjacentEdge(e);
		endNode.removeAdjacentEdge(e);
		edgeIDs.remove(e.getID());
		return edgesCache.remove(e.getID());
	}
	
	/**
	 * Removes all edges linked to a given node, as long as it is in the graph.
	 * @param n the node whose edges are to be removed.
	 */
	public void removeAllEdges(Node n) {
		if(!containsNodeID(n.getID()))
			return;
		for(Integer id : n.getAdjacentEdgesIDs()) {
			Edge e = this.getEdge(id);
			Node otherEnd = getNode(e.getOtherEndID(n));
			n.removeAdjacentEdge(e);
			otherEnd.removeAdjacentEdge(e);
			edgeIDs.remove(e.getID());
		}
	}
	
	/**
	 * Removes all elements associated with the graph. 
	 */
	public void clear() {
		nodeIDs.clear();
		edgeIDs.clear();
		nodesCache.removeAll();
		nodesCache.evictExpiredElements();
		edgesCache.removeAll();
		edgesCache.evictExpiredElements();
	}
	
	protected Cache getNodesCache() {
		return nodesCache;
	}
	
	protected Cache getEdgesCache() {
		return edgesCache;
	}
	
	/**
	 * Returns a {@link GraphView} instance containing the IDs of all currently present nodes and edges.
	 * @return a view containing the IDs of all currently present nodes and edges.
	 */
	public GraphView getGraphView() {
		GraphView view = new GraphView(this);
		view.addAllNodeIDs(nodeIDs.keySet());
		view.addAllEdgeIDs(edgeIDs.keySet());
		return view;
	}
	
	public void exportGraph(String destFilePath) throws IOException {
		exportGraphView(null, destFilePath);
	}
	
	/**
	 * Exports a view of the main graph (i.e. a subset of the nodes and a subset of the edges).
	 * @param view the view to be exported.
	 * @param destFilePath the destination file, which must have one of the following file extensions: '.gml' or '.graphml'.
	 * @throws IOException 
	 */
	public void exportGraphView(GraphView view, String destFilePath) throws IOException {
		boolean isGML = destFilePath.toLowerCase().endsWith("gml");
		boolean isGraphML = destFilePath.toLowerCase().endsWith("graphml");
		
		if(!isGML && !isGraphML) {
			throw new IllegalArgumentException("Invalid file type. " +
					"This function can only export to '.gml' or '.graphml' file types.");
		}
		
		initializeGephiGraph(view);
		
		if(isGML) {
			exportGraphToGML(destFilePath);
		} else {
			exportGraphToGraphML(destFilePath);
		}
		
		destroyGephiGraph();
	}
	
	/**
	 * Tests whether two graphs are the same, which is the case when their databases are the same, i.e. when their caches storing nodes 
	 * and the ones storing edges match.
	 * @param graph the graph to be compared.
	 * @return true, if the underlying caches match, false otherwise. 
	 */
	public boolean equals(Graph graph) {
		if(this.getNodesCache().equals(graph.getNodesCache()) && this.getEdgesCache().equals(graph.getEdgesCache()))
			return true;
		else
			return false;
	}
	
	private void exportGraphToGML(String destFilePath) throws IOException {
		if(!destFilePath.toLowerCase().endsWith(".gml"))
			destFilePath += ".gml";
		
		if(!gephiInitialized)
			initializeGephiGraph(null);
		
		ExportController ec = workspace.getLookup().lookup(ExportController.class);
		ExporterGML exporterGML = new ExporterGML();
		exporterGML.setWorkspace(workspace);
		exporterGML.setExportVisible(true);
		ProgressTicketProvider provider = new ProgressTicketProviderImpl();
		exporterGML.setProgressTicket(provider.createTicket("", null));
		StringWriter stringWriter = new StringWriter();
		ec.exportWriter(stringWriter, (CharacterExporter) exporterGML);
		PrintWriter writer = new PrintWriter(destFilePath, "UTF-8");
		writer.print(stringWriter.toString());
		writer.close();
		
	}
	
	private void exportGraphToGraphML(String destFilePath) throws IOException {
		if(!destFilePath.toLowerCase().endsWith(".graphml"))
			destFilePath += ".graphml";
		
		if(!gephiInitialized)
			initializeGephiGraph(null);
		
		ExportController ec = workspace.getLookup().lookup(ExportController.class);
		ExporterGraphML exporterGraphML = (ExporterGraphML) ec.getExporter("graphml");
		exporterGraphML.setWorkspace(workspace);
		exporterGraphML.setExportVisible(true);
		StringWriter stringWriter = new StringWriter();
		ec.exportWriter(stringWriter, (CharacterExporter) exporterGraphML);
		PrintWriter writer = new PrintWriter(destFilePath, "UTF-8");
		writer.print(stringWriter.toString());
		writer.close();
		
	}
	
	private void initializeGephiGraph(GraphView view) {
		pc = Lookup.getDefault().lookup(ProjectController.class);
		pc.newProject();
		workspace = pc.getCurrentWorkspace();
		
		workspace.add(Lookup.getDefault().lookup(GraphController.class));
		workspace.add(Lookup.getDefault().lookup(ExportController.class));
		
		graphModel = workspace.getLookup().lookup(GraphController.class).getModel();
		gephiGraph = graphModel.getUndirectedGraph();
		
		for(int id : getNodeIDs()) {
			if(view == null || view.containsNodeID(id)) {
				org.gephi.graph.api.Node n = graphModel.factory().newNode(""+id);
				MiningTools.setGephiNodeLabel(n, "id", "" + id);
				gephiGraph.addNode(n);
			}
		}
		
		for(int id : getEdgeIDs()) {
			if(view == null || view.containsEdgeID(id)) {
				Edge e = getEdge(id);
				org.gephi.graph.api.Node sourceNode = gephiGraph.getNode("" + e.getSourceID());
				org.gephi.graph.api.Node targetNode = gephiGraph.getNode("" + e.getTargetID());
				gephiGraph.addEdge(sourceNode, targetNode);
			}
		}
		gephiInitialized = true;
	}
	
	private void destroyGephiGraph() {
		if(pc != null) {
			pc.closeCurrentProject();
			workspace = null;
			graphModel = null;
			gephiGraph = null;
		}
		gephiInitialized = false;
	}
	
	private int generateEdgeID() {
		Random randGen = new Random();
		int next = 0;
		while(edgeIDs.containsKey(next)) {
			next = randGen.nextInt(Integer.MAX_VALUE);
		}
		return next;		
	}
}