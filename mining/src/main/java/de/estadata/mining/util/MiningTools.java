package de.estadata.mining.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import de.estadata.mining.datatransformation.Cluster;
import de.estadata.mining.datatransformation.Report;
import de.estadata.mining.graphmodel.*;

import org.apache.commons.lang3.StringUtils;

/**
 * This class provides a wide set of functionalities that can be used throughout the project (and outside it).
 * 
 * @author Nicolas Loza
 *
 */
public class MiningTools {
	
	private MiningTools() {	}
	
	/**
	 * The earth's radius in meters
	 */
	public static final double R = 6371.0 * 1000;
	
	/**
	 * Assigns cluster IDs to every node in the given graph (a connected component is a cluster).
	 * @param graph The graph containing the nodes.
	 * @param clusterIDLabel the label under which each node will contain its cluster ID.
	 */
	public static void assignClusterIDsOnGraph(Graph graph) {
		assignClusterIDsOnGraph(graph, null);
	}
	
	/**
	 * Assign cluster IDs to everz node in the given graph that is also in the associated graph view (a connected component 
	 * in the view is a cluster).
	 * @param graph The graph containing the nodes.
	 * @param clusterIDLabel the label under which each node will contain its cluster ID.
	 * @param view the view containing the nodes to be taken into account in the clustering.
	 */
	public static void assignClusterIDsOnGraph(Graph graph, GraphView view) {
		//Assign pairwise distinct cluster IDs to each connected component
		List<List<Integer>> connectedComponents = getConnectedComponents(graph, view);
		int clusterID = 1;
		int nodeCounter = 0;
		for(List<Integer> cc : connectedComponents) {
			if(cc.size() == 1) {
				Node v = graph.getNode(cc.get(0));
				v.setClusterID(-1);
				nodeCounter++;
			} else {
				for(Integer id : cc) {
					Node v = graph.getNode(id);
					v.setClusterID(clusterID);
					nodeCounter++;
				}
				clusterID++;
			}
		}
		if(nodeCounter != graph.getNodeCount()) {
			throw new IllegalStateException("nodeCounter = " + nodeCounter + ", graphNodes = " + graph.getNodeCount());
		}
	}
	
	/**
	 * Returns a list of the connected components in the given graph. In turn, a connected component is represented as a list of the node IDs in
	 * said component.
	 * @param graph the graph containing the nodes.
	 * @return a list of connected components (lists of nodes).
	 */
	public static List<List<Integer>> getConnectedComponents(Graph graph) {
		return getConnectedComponents(graph, null);
	}
	
	/**
	 * Returns a list of the connected components in the given graph under a given view.
	 * @param graph the graph containing all the nodes.
	 * @param view the view containing the nodes to be taken into account.
	 * @return a list of connected components (lists of nodes).
	 */
	public static List<List<Integer>> getConnectedComponents(Graph graph, GraphView view) {
		Map<Integer, Integer> visitedNodes = new HashMap<Integer, Integer>(graph.getNodeCount());
		List<List<Integer>> connectedComponents = new ArrayList<List<Integer>>();
		for(Integer nodeID : graph.getNodeIDs()) {
			if(!visitedNodes.containsKey(nodeID)) {
				List<Integer> cc = getComponentIDs(nodeID, graph, view, visitedNodes);
				connectedComponents.add(cc);
			}
		}
		return connectedComponents;
	}
	
	private static List<Integer> getComponentIDs(int nodeID, Graph graph, GraphView view, Map<Integer, Integer> visitedNodes) {
		List<Integer> component = new ArrayList<Integer>();
		LinkedList<Node> queue = new LinkedList<Node>();
		queue.add(graph.getNode(nodeID));
		while(!queue.isEmpty()) {
			Node v = queue.poll();
			if(!visitedNodes.containsKey(v.getID())) {
				component.add(v.getID());
				visitedNodes.put(v.getID(), v.getID());
				for(Integer id : v.getAdjacentEdgesIDs()) {
					Edge e = graph.getEdge(id);
					int otherEndID = e.getOtherEndID(v);
					if(view != null) {
						if(view.containsEdgeID(e.getID())) {
							if(!visitedNodes.containsKey(otherEndID)) {
								queue.add(graph.getNode(otherEndID));
							}
						}
					} else {
						if(!visitedNodes.containsKey(otherEndID)) {
							queue.add(graph.getNode(otherEndID));
						}
					}
				}
			}
		}
		return component;
	}
	
	/**
	 * Adds a {@link Report} to the corresponding cluster (in this case represented as a list) in a map. 
	 * The report should have been previously assigned a cluster ID.
	 * @param p The report to be assigned to a cluster.
	 * @param multiMap The map containing as keys the cluster IDs and as values the corresponding clusters (represented as lists of reports).
	 */
	public static void addReportToClustermap(Report p, Map<Integer, List<Report>> multiMap) {
		List<Report> list;
		if (multiMap.containsKey(p.getClusterID())) {
			list = multiMap.get(p.getClusterID());
			list.add(p);
		} else {
			list = new ArrayList<Report>();
			list.add(p);
			multiMap.put(p.getClusterID(), list);
		}
	}
	
	/**
	 * Sort a collection into a list.
	 * @param c the collection to be sorted.
	 * @return a list with the sorted collection's element.
	 */
	public static <T extends Comparable<? super T>> List<T> asSortedList(
			Collection<T> c) {
		List<T> list = new ArrayList<T>(c);
		java.util.Collections.sort(list);
		return list;
	}
	
	/**
	 * Sets a value for a certain attribute/label name within a certain {@link Node}. 
	 * [Note: in some cases, because of faulty configurations of the Terracotta Caches, attributes can disappear after 
	 * storing the cache and reloading it to memory. For more information, see Terracotta documentation]  
	 * @param v the node.
	 * @param label the label name.
	 * @param value the value to be stored under the label.
	 */
	public static void setNodeLabel(Node v, String label, String value) {
		v.setAttribute(label, value);
	}

	/**
	 * Sets a value for an attribute/label name within some Gephi node (not a {@link Node}). For more information, see
	 * Gephi documentation.
	 * @param gephiNode The Gephi node.
	 * @param label the label name.
	 * @param value the value to be stored under the label.
	 */
	public static void setGephiNodeLabel(org.gephi.graph.api.Node gephiNode, String label, String value) {
		gephiNode.getAttributes().setValue(label, value);	
	}
	
	/**
	 * Tests whether a {@link Node} has a certain label value set to true.
	 * @param v the node.
	 * @param label the label name.
	 * @return true, if the label value is 'true', false otherwise.
	 */
	public static boolean nodeIsLabeledAs(Node v, String label) {
		String value = getNodeLabelValue(v, label).toLowerCase(); 
		return (value != null) ? value.equals("true") : false;
	}
	
	/**
	 * Tests whether a Gephi node has a certain label value set to true.
	 * @param gephiNode the Gephi node.
	 * @param label the label name.
	 * @return true, if the label value is 'true', false otherwise.
	 */
	public static boolean gephiNodeIsLabeledAs(org.gephi.graph.api.Node gephiNode, String label) {
		String value = getGephiNodeLabelValue(gephiNode, label).toLowerCase(); 
		return (value != null) ? value.equals("true") : false;
	}
	
	/**
	 * Returns the value stored in a {@link Node} under the given label name.
	 * @param v the node.
	 * @param label the label name.
	 * @return the value stored under the given label name (null if no value available).
	 */
	public static String getNodeLabelValue(Node v, String label) {
		return v.getAttribute(label);
	}
	
	/**
	 * Returns the value stored in a Gephi node under the given label value. 
	 * @param gephiNode the Gephi node.
	 * @param label the label name.
	 * @return the value stored under the given label name (null if no value available).
	 */
	public static String getGephiNodeLabelValue(org.gephi.graph.api.Node gephiNode, String label) {
		Object value = gephiNode.getAttributes().getValue(label);
		return (value != null) ? "" + value : null;
	}
	
	/**
	 * Sets a value for a label within a Gephi edge.
	 * @param gephiEdge the Gephi edge.
	 * @param label the label name.
	 * @param value the value to be set under the label name.
	 */
	public static synchronized void setEdgeLabel(org.gephi.graph.api.Edge gephiEdge, String label, String value) {
		gephiEdge.getAttributes().setValue(label, value);
	}
	
	/**
	 * Returns the value stored under a certain label name within a certain Gephi edge. 
	 * @param gephiEdge the Gephi Edge.
	 * @param label the label name.
	 * @return a String with the value stored under the label name (null if none found).
	 */
	public static String getEdgeLabelValue(org.gephi.graph.api.Edge gephiEdge, String label) {
		Object value = gephiEdge.getAttributes().getValue(label);
		return (value != null) ? "" + value : null;
	}

	/**
	 * Tests whether a Gephi edge has a label set to true.
	 * @param gephiEdge the Gephi Edge.
	 * @param label the label name.
	 * @return true if the value stored under the label name is 'true', false otherwise.
	 */
	public static boolean edgeIsLabeledAs(org.gephi.graph.api.Edge gephiEdge, String label) {
		String value = getEdgeLabelValue(gephiEdge, label).toLowerCase();
		return (value != null) ? value.equals("true") : false;
	}
	
	/**
	 * Parses a String to a Date.
	 * @param createdAtAsString the string to be parsed.
	 * @param format the date format of the string.
	 * @return the Date equivalent to the given String or null if it cannot be parsed to a Date.
	 */
	public static Date getCreatedAt(String createdAtAsString, String format) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			if(createdAtAsString != null) {
				cal.setTime(sdf.parse(createdAtAsString));
			} else {
				return null;
			}
			
		} catch (ParseException e) {
			return null;
		}
		return cal.getTime();
	}
	
	/**
	 * Computes the (Haversine) spatial distance in meters between two {@link Report}s .
	 * @param r1 the first report.
	 * @param r2 the second report.
	 * @return the spatial distance in meters (rounded down).
	 */
	public static int getSpaceDistance(Report r1, Report r2) {
		double R = 6371;
		
		double latn1 = r1.getLat();
		double longn1 = r1.getLon();
		double latn2 = r2.getLat();
		double longn2 = r2.getLon();
		
		double dLat = Math.toRadians(latn1 - latn2);
		double dLon = Math.toRadians(longn1 - longn2);
		double lat1 = Math.toRadians(latn1);
		double lat2 = Math.toRadians(latn2);
		
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
		a = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		a = R * a;
		a = a * 1000;
		
		return (int) a;
	}
	
	/**
	 * Computes the Levenshtein Similarity between two texts.
	 * @param text1 the first text.
	 * @param text2 the second text.
	 * @return a value in range [0,1], where values close to the 1 mean high similarity.
	 */
	public static double getLevenshteinSimilarity(String text1, String text2) {
		//1 - d(str1,str2) / max(A,B)
		double levDist = (double) StringUtils.getLevenshteinDistance(text1, text2);
		return 1 - (levDist)/Math.max(text1.length(), text2.length());
	}
	
//	private static List<Object> getCacheObjectsAsList(Cache cache) {
//		if(cache == null)
//			return new ArrayList<>();
//		Map<Object, Element> all = cache.getAll(cache.getKeys());
//		List<Object> elementValues = new ArrayList<Object>(all.size());
//		
//		for(Entry<Object, Element> entry : all.entrySet()) {
//			elementValues.add(entry.getValue().getObjectValue());
//		}
//		return elementValues;
//	}
	
	@SuppressWarnings("unchecked")
	/**
	 * Returns a list containing all objects stored in a Terracotta Cache that belong or are assignable to a certain class.
	 * @param cache the Terracotta cache containing the objects.
	 * @param genericClass the (possibly super-) class of the objects to be returned.
	 * @param keys a set of keys. If null, all of the cache's assignable objects are returned.
	 * @return a list of all cache objects that are assignable to the given class.
	 */
	public static <T> List<T> getCacheObjectsAsList(Cache cache, Class<T> genericClass, List<Integer> keys) {
		List<T> list = new ArrayList<T>();
		Map<Object, Element> map = (keys != null) ? getCacheObjectsAsMap(cache, keys) : getCacheObjectsAsMap(cache, cache.getKeys()); 
		for(Entry<Object, Element> entry : map.entrySet()) {
			Object val = entry.getValue().getObjectValue();
			if(genericClass.isAssignableFrom(val.getClass())) {
				list.add((T) val);
			}
		}
		return list;
	}
	
	private static Map<Object, Element> getCacheObjectsAsMap(Cache cache, List<Integer> keys) {
		if(cache == null)
			return new HashMap<Object, Element>();
		return cache.getAll(keys);
	}
	
	@SuppressWarnings("unchecked")
	/**
	 * Returns a map containing all objects stored in a Terracotta Cache that belong or are assignable to a certain class.
	 * A list of keys can also be provided, meaning that all elements stored in the cache whose keys are not in said list 
	 * are ignored and not returned. If the list is null, all cache elements are analyzed. 
	 * @param cache the Terracotta Cache containing the objects.
	 * @param keys a possible subset of all the cache Keys to be analyzed.
	 * @param genericClass the (possibly super-) class of the objects to be returned.
	 * @return a map of all cache objects that are assignable to the given class.
	 */
	public static <T> Map<Object, T> getCacheObjectsAsMap(Cache cache, List<Integer> keys, Class<T> genericClass) {
		Map<Object, T> map = new HashMap<Object, T>();
		if(keys == null)
			keys = cache.getKeys();
		for(Entry<Object, Element> entry : getCacheObjectsAsMap(cache, keys).entrySet()) {
			if(genericClass.isAssignableFrom(entry.getValue().getClass())) {
				map.put(entry.getKey(), (T) entry.getValue());
			}
		}
		return map;
	}
	
	/**
	 * Returns a list containing the names of all the currently present Terracotta Caches under a CacheManager.
	 * @param databaseManager the CacheManager containing the caches.
	 * @return a list with all cache names.
	 */
	public static List<String> getCacheNames(CacheManager databaseManager) {
		return Arrays.asList(databaseManager.getCacheNames());
	}
	
	
	/*
	 *  -------p1------
	 *  |              |
	 *  p3   origin    p4
	 *  |              |
	 *  -------p2------
	 */
	/**
	 * Returns a bounding box for a certain report. Said box has the form of an array {{minLat,minLon}, {maxLat,maxLon}}.
	 * The values are calculated as follows: p1 = shift(origin, dist, 0), p2 = shift(origin, dist, 180), p3 = shift(origin, dist, 90), 
	 * p4 = shift(origin, dist, 270), where shift() returns the point (lat, lon) resulting of shifting the origin 'dist' meters in the direction 
	 * given by the third parameter (degrees). Then we obtain: minLat = min(p1.lat,..., p4.lat), minLon = min(p1.lon,..., p4.lon). Analogously
	 * we obtain maxLat and maxLon using max() instead of min().  
	 * @param rep the origin
	 * @param perpendicularDistance the distance that affects the size of the bounding box.
	 * @return an array of the form [[minLat,minLon], [maxLat,maxLon]].
	 */
	public static double[][] getBoundingBox(Report rep, int perpendicularDistance) {
		double[] origin = {rep.getLat(), rep.getLon()};
		double[] p1 = shiftCoords(origin, perpendicularDistance, 0);
		double[] p2 = shiftCoords(origin, perpendicularDistance, 180);
		double[] p3 = shiftCoords(origin, perpendicularDistance, 90);
		double[] p4 = shiftCoords(origin, perpendicularDistance, 270);
		
		double minLat = Math.min(p1[0], Math.min(p2[0], Math.min(p3[0], p4[0])));
		double maxLat = Math.max(p1[0], Math.max(p2[0], Math.max(p3[0], p4[0])));
		double minLon = Math.min(p1[1], Math.min(p2[1], Math.min(p3[1], p4[1])));
		double maxLon = Math.max(p1[1], Math.max(p2[1], Math.max(p3[1], p4[1])));
		
		double[][] boundingBox = {{minLat, minLon}, {maxLat, maxLon}};
		return boundingBox;
	}
	
	//Returns an array {latitude, longitude} with the new coordinates in degrees, with a specific distance (in meters) from the origin.
	//The bearing specifies in which direction the origin should be 'shifted'.
	//See http://www.movable-type.co.uk/scripts/latlong.html, function rhumbDestinationPoint()
	//Use 0 as bearing to increase only the latitude, 90 to increase only the longitude
	/**
	 * Returns an array with the new coordinates (in degrees) {lat, lon}. This new point has a specific distance in meters from the origin.
	 * The bearing specifies in which direction the origin should be shifted.
	 * @param origin The point to be shifted.
	 * @param distance distance the point will be shifted.
	 * @param bearing the direction in which the point will be shifted. 
	 * @return an array with the new coordinates.
	 */
	public static double[] shiftCoords(double[] origin, double distance, double bearing) {
		double delta = distance / R;
		double phi1 = Math.toRadians(origin[0]);
		double lambda1 = Math.toRadians(origin[1]);
		double theta = Math.toRadians(bearing);
		
		double dPhi = delta * Math.cos(theta);
		
		double phi2 = phi1 + dPhi;
		
		if(Math.abs(phi2) > Math.PI / 2)
			phi2 = (phi2 > 0) ? Math.PI - phi2 : -Math.PI - phi2;
		
		double dPsi = Math.log(Math.tan(phi2 / 2 + Math.PI / 4) / Math.tan(phi1 / 2 + Math.PI / 4));
		double q = Math.abs(dPsi) > Math.pow(10, -12) ? dPhi / dPsi : Math.cos(phi1);
		
		double dLambda = delta * Math.sin(theta) / q;
		
		double lambda2 = lambda1 + dLambda;
		
		lambda2 = (lambda2 + 3*Math.PI) % (2*Math.PI) - Math.PI;
		
		double[] result = {Math.toDegrees(phi2), Math.toDegrees(lambda2)};
		return result;
	}

	/**
	 * Sends an Email using SSL. Note: currently only sending from gmail accounts is supported.
	 * @param sender the Gmail Email address of the sender. 
	 * @param pw the password of the sender address.
	 * @param to 
	 * @param subject
	 * @param stringMessage
	 */
	public static void sendMailSSL(final String sender, final String pw, String to, String subject, String stringMessage) {
		
		//Get the session object
		Properties props = new Properties();
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", "465");
		 
		Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(sender, pw);
			}
		});
		//compose message
		try {
		 MimeMessage message = new MimeMessage(session);
		 message.setFrom(new InternetAddress(sender));
		 message.addRecipient(Message.RecipientType.TO,new InternetAddress(to));
		 message.setSubject(subject);
		 message.setText(stringMessage);
		 
		 //send message
		 Transport.send(message);
		 
		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
		 
	}
	
	/**
	 * This method export (writes) the clustering results as CSV. Two files are
	 * generated by this method: 1) pointsPath = Original Data (Points) with
	 * extra column containing cluster label 2) clustersPath = Cluster center
	 * coordinates and cluster size
	 * 
	 * 
	 * @param reports
	 *            - the Clustering Result
	 * @param pointsFilePath
	 *            - (1) File Path to Write
	 * @param clusterFilePath
	 *            - (2) File Path to Write
	 * @param clusters
	 * 			  - the clusters to be exported (can be <code>null<code>)
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public static void resultsToCSV(List<Report> reports,
			String pointsFilePath, String clusterFilePath, List<Cluster> clusters)
			throws FileNotFoundException, UnsupportedEncodingException {
		if(clusters == null || clusters.size() == 0) {
			clusters = generateClusters(reports);
		}
		PrintWriter writer = new PrintWriter(clusterFilePath, "UTF-8");
		//writer.write(Cluster.CSV_Header());
		writer.write(Cluster.CSV_Extended_Header());
		for (Cluster cluster : clusters) {
			if (cluster.clusterLabel > 0)
				//writer.write(cluster.toCSV());
				writer.write(cluster.toCSVExtended());
		}
		writer.close();

		writer = new PrintWriter(pointsFilePath, "UTF-8");
		writer.write(Report.CSV_Header());
		for (Cluster cluster : clusters) {
			for (Report point : cluster.reports) {
				writer.write(point.toCSV());
			}
		}
		writer.close();
	}

	private static List<Cluster> generateClusters(List<Report> points) {
		HashMap<Integer, List<Report>> clusterTable = new HashMap<Integer, List<Report>>();
		for (Report p : points) {
			MiningTools.addReportToClustermap(p, clusterTable);
		}

		List<Cluster> clusters = new ArrayList<Cluster>();
		for (Integer key : MiningTools.asSortedList(clusterTable.keySet())) {
			List<Report> list = clusterTable.get(key);
			Cluster cluster = new Cluster(list, key);
			clusters.add(cluster);
		}
		return clusters;
	}
	
}
