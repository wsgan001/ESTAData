package de.estadata.mining.datatransformation;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Licensed to Creative Commons ShareAlike 4.0 International (CC BY-SA 4.0)
 * */

/**
 * This class represents a cluster containing the Reports. Objects of this class
 * are created after the clustering results.
 * 
 * @author Julio De Melo Borges
 * @author Nicolas Loza
 */
public class Cluster implements Serializable {
	class TemporalDiameter {
		long temporalDiameter;
		Date startDate;
		Date endDate;
		public TemporalDiameter(long temporalDiameter, Date startDate,
				Date endDate) {
			super();
			this.temporalDiameter = temporalDiameter;
			this.startDate = startDate;
			this.endDate = endDate;
		}
	}

	private static final long serialVersionUID = -8006963561643543354L;
	@Expose
	public List<Report> reports;

	@Expose
	@SerializedName("label")
	public int clusterLabel;

	@Expose
	public long minTime;

	@Expose
	public long maxTime;

	@Expose
	public double[] boundingBox = null;// TODO

	@Expose
	@SerializedName("lat")
	public double latCenter;

	@Expose
	@SerializedName("lng")
	public double lonCenter;

	public Cluster(List<Report> list, int clusterLabel) {
		this.reports = list;
		this.clusterLabel = clusterLabel;
		this.minTime = minTime();
		this.maxTime = maxTime();
		this.latCenter = latCenter();
		this.lonCenter = lonCenter();
	}

	public int size() {
		return reports.size();
	}

	public double latCenter() {
		double latSum = 0;
		for (Report point : reports) {
			latSum += point.getLat();
		}
		return latSum / size();
	}

	public double lonCenter() {
		double lonSum = 0;
		for (Report point : reports) {
			lonSum += point.getLon();
		}
		return lonSum / size();
	}

	public long minTime() {
		long mintime = Long.MAX_VALUE;
		for (Report point : reports) {
			if (point.getCreationTime() < mintime)
				mintime = point.getCreationTime();
		}
		return mintime;
	}

	public long maxTime() {
		long maxtime = Long.MIN_VALUE;
		for (Report point : reports) {
			if (point.getCreationTime() > maxtime)
				maxtime = point.getCreationTime();
		}
		return maxtime;
	}

	// TODO
	public double[] boundingBox() {
		return null;
	}

	public static String CSV_Header() {
		return "label,lat,lon,size\n";
	}

	public String toCSV() {
		return clusterLabel + "," + latCenter + "," + lonCenter + "," + size()
				+ "\n";
	}

	public double spatialDiameter() {
		double spatialDiameter = 0;
		for (int i = 0; i < reports.size(); i++) {
			for (int j = i + 1; j < reports.size(); j++) {
				Report p1 = reports.get(i);
				Report p2 = reports.get(j);
				double dist = p1.spatialDistanceFromPoint(p2);
				if (dist >= spatialDiameter)
					spatialDiameter = dist;
			}
		}
		return spatialDiameter;
	}

	public TemporalDiameter temporalDiameter() {
		long temporalDiameter = 0;
		Date startDate = null;
		Date endDate = null;
		for (int i = 0; i < reports.size(); i++) {
			for (int j = 0; j < reports.size(); j++) {
				Report p1 = reports.get(i);
				Report p2 = reports.get(j);
				long dist = p1.temporalDistanceFromPoint(p2);
				if (dist >= temporalDiameter) {
					temporalDiameter = dist;
					if (p1.getCreationDate().compareTo(p2.getCreationDate()) < 0) {
						startDate = p1.getCreationDate();
						endDate = p2.getCreationDate();
					} else {
						startDate = p2.getCreationDate();
						endDate = p1.getCreationDate();
					}
				}
			}
		}
		return new TemporalDiameter(temporalDiameter, startDate, endDate);
	}

	public String category() {
		return this.reports.get(0).getText();
	}

	public String clusterDescription() {
		String desc = "";
		for (Report p1 : this.reports) {
			desc = desc + p1.getUrl() + "; ";
		}
		return desc;
	}

	public static String CSV_Extended_Header() {
		return "label,latitude,longitude,size,sptDiameter,tempDiameter,created_at,updated_at,category,desc,\n";
	}

	@SuppressWarnings("deprecation")
	public String toCSVExtended() {
		TemporalDiameter temporalDiameter = temporalDiameter();
		return clusterLabel + "," + latCenter() + "," + lonCenter() + ","
				+ size() + "," + spatialDiameter() + ","
				+ temporalDiameter.temporalDiameter + ","
				+ temporalDiameter.startDate.toGMTString() + ","
				+ temporalDiameter.endDate.toGMTString() + "," + category()
				+ "," + clusterDescription() + "\n";
	}

	// GETTERS AND SETTERS
	public List<Report> getReports() {
		return reports;
	}

	public void setPoints(ArrayList<Report> points) {
		this.reports = points;
	}

	public int getLabel() {
		return clusterLabel;
	}

	public void setLabel(int label) {
		this.clusterLabel = label;
	}

	public long getMinTime() {
		return minTime;
	}

	public void setMinTime(long minTime) {
		this.minTime = minTime;
	}

	public long getMaxTime() {
		return maxTime;
	}

	public void setMaxTime(long maxTime) {
		this.maxTime = maxTime;
	}

	public double[] getBoundingBox() {
		return boundingBox;
	}

	public void setBoundingBox(double[] boundingBox) {
		this.boundingBox = boundingBox;
	}

	public double getLatCenter() {
		return latCenter;
	}

	public void setLatCenter(double latCenter) {
		this.latCenter = latCenter;
	}

	public double getLonCenter() {
		return lonCenter;
	}

	public void setLonCenter(double lonCenter) {
		this.lonCenter = lonCenter;
	}
}
