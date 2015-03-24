package de.estadata.mining.datatransformation;

import com.google.gson.annotations.Expose;


import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Licensed to Creative Commons ShareAlike 4.0 International (CC BY-SA 4.0)
 * */

/**
 * The class models the spatio-temporal points to be clustered. It possses
 * spatio, temporal and semantic parameters as well as parameters related to the
 * clustering itself. Methods for comparing points w.r.t to these parameters are
 * included in this class.
 * 
 * @author De Melo Borges
 * @author Nicolas Loza
 */
public class Report implements Serializable, Comparable<Report> {
	private static final long serialVersionUID = -8464587919845078355L;
	private int index;
	@Expose
	private int userId;
	// SPATIAL ATTRIBUTES:
	@Expose
	private double lat;
	@Expose
	private double lon;
	private double x;
	private double y;
	private double z;
	// TEMPORAL ATTRIBUTE
	@Expose
	private Date creationDate;
	@Expose
	private long creationTime;
	// SEMANTIC ATTRIBUTES
	@Expose
	private String text;
	@Expose
	private String category;
	@Expose
	private String url;
	// CLUSTERING ATTRIBUTES
	private boolean isNoise = false;
	private boolean isDuplicate = false;
	@Expose
	private int clusterID = -1;
	// Radius of the earth in meters
	private static final double R = 6371.0 * 1000;
	
//	private Hashtable<Integer, Edge> edges;
	
	@Expose
	private int id;

	public Report(double lat, double lon, String text, String category,
			String url, Date creationDate, String clusterId, int key) {
		this.lat = lat;
		this.lon = lon;
		this.text = text;
		this.category = category;
		this.url = url;
		this.creationDate = creationDate;
		this.creationTime = creationDate.getTime();
		this.id = key;
		if (clusterId != null)
			this.clusterID = new Integer(clusterId);

		isDuplicate = text.toLowerCase().contains("duplicate");
		
	}
	
//	public void addEdge() {
//		Random rand = new Random();
//		edges.put(rand.nextInt(), new Edge(rand.nextInt(), rand.nextInt()));
//	}
//	
//	public int getTableSize() {
//		return edges.size();
//	}

	public Report(double lat, double lon, String text, String category,
			String url, Date creationDate, int key, int userId) {
		this.lat = lat;
		this.lon = lon;
		this.text = text;
		this.category = category;
		this.url = url;
		this.creationDate = creationDate;
		this.creationTime = creationDate.getTime();
		this.id = key;
		this.clusterID = -1;
		this.userId = userId;

		isDuplicate = text.toLowerCase().contains("duplicate");
		
	}

	/**
	 * Get the difference between two dates in days
	 * 
	 * @param t
	 *            - the comparing point.
	 * @return the difference value in days.
	 */
	public long temporalDistanceFromPoint(Report t) {
		Date date1 = creationDate;
		Date date2 = t.creationDate;
		TimeUnit timeUnit = TimeUnit.DAYS;
		long diffInMillies = date2.getTime() - date1.getTime();
		return Math.abs(timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS));
	}

	/**
	 * Calls the suitable method for calculating the spatial distance between
	 * two points. It depends on STDBSCAN.isEuclidian. If this is flagged to
	 * true, the KD-Tree of the class will calculate distances based on the
	 * euclidian paremeters, so the method should delivers the euclidian based
	 * distance as well. Otherwise delivers the haversine distance based on the
	 * coordinates.
	 * 
	 * @param t
	 *            - the comparing point.
	 * @return the spatial distance in meters.
	 */
	public double spatialDistanceFromPoint(Report t) {
		// if (Report.isEuclidian)
		// return euclidianDistance(t);
		// else
		return haversineDist(t);
	}

	/**
	 * Euclidian Distance
	 * 
	 * @param t
	 *            - the comparing point.
	 * @return the spatial distance in meters.
	 */
	/*private double euclidianDistance(Report t) {
		return Math.sqrt(Math.pow(x - t.x, 2) + Math.pow(y - t.y, 2)
				+ Math.pow(z - t.z, 2));
	}*/

	/**
	 * Haversine Distance based on coordinates.
	 * 
	 * @param t
	 *            - the comparing point.
	 * @return the spatial distance in meters.
	 */
	public double haversineDist(Report t) {
		double lat1 = this.lat;
		double lng1 = this.lon;
		double lat2 = t.lat;
		double lng2 = t.lon;
		double dLat = Math.toRadians(lat2 - lat1);
		double dLng = Math.toRadians(lng2 - lng1);
		double sindLat = Math.sin(dLat / 2);
		double sindLng = Math.sin(dLng / 2);
		double a = Math.pow(sindLat, 2) + Math.pow(sindLng, 2)
				* Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2));
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double dist = (R * c);
		return dist;
	}

	/**
	 * Utility method to transform the geographical coordinates to euclidian
	 * coordinates.
	 * 
	 * @param lat
	 * @param lon
	 */
	/*private void gisToEuclidian(double lat, double lon) {
		lat *= Math.PI / 180.0;
		lon *= Math.PI / 180.0;

		this.x = R * Math.cos(lat) * Math.cos(lon);
		this.y = R * Math.cos(lat) * Math.sin(lon);
		this.z = R * Math.sin(lat);
	}
*/
	/*
	 * public double levenshteinSim(STPoint t) { //1 - d(str1,str2) / max(A,B)
	 * double levDist = (double) StringUtils.getLevenshteinDistance(this.text,
	 * t.text); return 1 - (levDist)/Math.max(this.text.length(),
	 * t.text.length()); }
	 */

	/**
	 * Euclidian coordinates as array. Used as key entry by the KD-Tree
	 * 
	 * @return array containing euclidian coordinates.
	 */
	public double[] toDouble() {
		double[] xyz = { x, y, z };
		return xyz;
	}

	/**
	 * Utility method needed for exporting points to CSV
	 * 
	 * @return the CSV String, separated by commas
	 */
	public String toCSV() {
		return index + "," + dateFormated() + "," + lat + "," + lon + ","
				+ category + "," + text.replaceAll(",", ";") + "," + url + ","
				+ isDuplicate + "," + clusterID + "\n";

	}

	/**
	 * Utility method needed to delivers the date (creation date) in suitable
	 * format for file writing
	 * 
	 * @return formatted date as String
	 */
	private String dateFormated() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		return simpleDateFormat.format(this.creationDate);

	}

	/**
	 * The Header for CSV File
	 * 
	 * @return
	 */
	public static String CSV_Header() {
		return "index,creationDate,lat,lon,category,text,url,duplicate,clusterLabel\n";
	}

	public boolean equals(Report t) {
		return (lat == t.lat) && (lon == t.lon);
	}

	public String toString() {
		// return "Created at: " + this.creationDate.toString()
		// + ". Coordinates: " + this.lat + ", " + this.lon;
		return "" + id;
	}

	// GETTERS AND SETTERS

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public int getClusterID() {
		return clusterID;
	}

	public void setClusterID(int clusterID) {
		this.clusterID = clusterID;
	}

	public int getID() {
		return id;
	}

	public void setID(int id) {
		this.id = id;
	}

	public boolean isDuplicate() {
		return this.isDuplicate;
	}

	public int getUserId() {
		return userId;
	}

	public boolean isNoise() {
		return isNoise;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public void setNoise(boolean isNoise) {
		this.isNoise = isNoise;
	}

	public void setDuplicate(boolean isDuplicate) {
		this.isDuplicate = isDuplicate;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int compareTo(Report o) {
		return creationDate.compareTo(o.creationDate);
	}
}
