package de.estadata.mining.util;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import net.sf.ehcache.Cache;

import de.estadata.mining.datatransformation.Cluster;
import de.estadata.mining.datatransformation.Report;

public class Evaluation {
	
	public static String runAnalysis(Cache pointsCache,
			Cache clustersCache,
			long time, 
			String algorithmConfiguration) throws FileNotFoundException, UnsupportedEncodingException {
		
		List<Report> reports = MiningTools.getCacheObjectsAsList(pointsCache, Report.class, null);
		int total = reports.size();
		int totalInCluster = 0;
		float fp = 0;
		float tp = 0;
		float tn = 0;
		float fn = 0;
		
		for (Report p : reports) {
			if (p.isDuplicate() && p.getClusterID()>0) {
				tp++; 
				totalInCluster++;
			} else if (!p.isDuplicate() && p.getClusterID() > 0) {
				fp++;
				totalInCluster++;
			} else if (p.isDuplicate() && !(p.getClusterID() > 0)) {
				fn++;
			} else if (!p.isDuplicate() && !(p.getClusterID() > 0)) {
				tn++;
			}
		}
		
		float precision = tp/(tp + fp);
		float recall = tp/(tp + fn);
		float fmeasure = (2 * precision * recall) / (precision + recall);
		
		List<Cluster> clusters = MiningTools.getCacheObjectsAsList(clustersCache, Cluster.class, null);
		
		double purity = purity(clusters);
		double compression = compression(reports);
		double entropy = entropyDuplicate(reports);
		
		String analysis = "\nThe algorithm took " + time + " ms to complete\n";
		analysis += "Configuration: " + algorithmConfiguration + "\n";
		analysis += "#Points: " + total + ", #points in clusters: " + totalInCluster + ", #clusters: " + clusters.size() + "\n";
		analysis += "tp: " + tp + ", fp: " + fn + ", tn: " + tn + ", fn: " + fn + "\n";
		analysis += "Precision: " + precision + ", recall: " + recall + ", F-Measure: " + fmeasure + "\n";
		analysis += "Purity: " + purity + "\n";
		analysis += "Compression: " + compression + "\n";
		analysis += "Entropy: " + entropy + "\n";
		
		return analysis;		
	}
	
	private static double purity(List<Cluster> clusters) {
		int purityInt = 0;
		int size = 0;
		for (Cluster cluster : clusters) {
			purityInt = (int) (purityInt + clusterPurity(cluster));
			size = size + cluster.reports.size();
		}
		return (double)((double)purityInt/(double)size);
	}
	
	private static int clusterPurity(Cluster cluster) {
		int trueLabel=0;
		int falseLabel=0;
		for (Report report : cluster.reports) {
			if (report.isDuplicate())
				trueLabel++;
			else
				falseLabel++;
		}
		return Math.max(trueLabel, falseLabel);
	}
	
	private static double compression(List<Report> points) {
		int originalSize = points.size();
		int clustersAmount = 0;
		int outliersAmount = 0;
		for (Report report : points) {
			if (report.getClusterID()>clustersAmount)
				clustersAmount = report.getClusterID();
			if (report.getClusterID()==-1)
				outliersAmount = outliersAmount+1;
		}
		return (double)((double)(outliersAmount+clustersAmount)/(double)originalSize);
	}
	
	private static double entropyDuplicate(List<Report> points) {
		ArrayList<Boolean> labels = new ArrayList<Boolean>();
		double duplicateCount = 0;
		for (Report report : points) {
			labels.add(report.isDuplicate());
			labels.add(report.getClusterID()>0);
			if (report.isDuplicate())
				duplicateCount++;
				
		}
		double nonduplicateCount = points.size() - duplicateCount;
		double total = points.size();
		
		double p1 = (nonduplicateCount/total);
		double l1 = Math.log(nonduplicateCount/total);
		double p2 = (duplicateCount/total);
		double l2 = Math.log(duplicateCount/total);
		
		return -(p1*l1)-(p2*l2);
	}
	
}
