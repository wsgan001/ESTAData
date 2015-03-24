package de.estadata.mining.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import au.com.bytecode.opencsv.CSVReader;
import de.estadata.mining.datatransformation.Report;

/**
 * This class provides the necessary methods for loading report data from csv and json files that fulfill certain formats.
 * 
 * @author Nicolas Loza
 *
 */
public class DataLoader {
	private static final String lngColumn = "lng";
	private static int lngColumnIndex;
		
	private static final String latColumn = "lat";
	private static int latColumnIndex;
		
	private static final String createdAtColumn = "created_at";
	private static int createdAtColumnIndex;
	
	private static final String categoryColumn = "summary";
	private static int categoryColumnIndex;
	
	private static final String textColumn = "description";
	private static int textColumnIndex;
	
	private static final String urlColumn = "bitly";
	private static int urlColumnIndex;
	
	private static final String reportIDColumn = "id";
	private static int reportIDColumnIndex;
	
	private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	private DataLoader() {}
	
	public static boolean load(String dataType, String dataPath, Cache reportsCache, double ratio) throws IOException, InterruptedException, ExecutionException {
		if(ratio < 0 || ratio > 1.0) {
			throw new IllegalArgumentException("Invalid ratio: " + ratio + ". Should be a value between 0 and 1.");
		}
		boolean loadCSV;
		if(dataType.toLowerCase().equals("csv"))
			loadCSV = true;
		else if(dataType.toLowerCase().equals("json"))
			loadCSV = false;
		else
			throw new IllegalArgumentException("Invalid file type: " + dataType + "; only 'csv' and 'json' are accepted.");
		
		if(loadCSV)
			loadCSVData(dataPath, reportsCache, ratio);
		else
			loadJSONData(dataPath, reportsCache, ratio);
		
		return true;
	}
	
	private static void loadCSVData(String path, Cache reportsCache, double ratio) throws IOException, InterruptedException, ExecutionException  {
		Random rand = new Random();
		File f = new File(path);
		List<String> csvFilePaths = new ArrayList<String>();
		
		if(f.isDirectory()) {
			for(File file : f.listFiles()) {
				if(file.getAbsolutePath().toLowerCase().endsWith(".csv"))
					csvFilePaths.add(file.getAbsolutePath());
			}
		} else {
			if(!path.toLowerCase().endsWith("csv"))
				throw new IllegalStateException("Invalid path: " + path + ". Must be either a path to a directory or to a .csv file");
			csvFilePaths.add(path);
		}
		
		int repCount = 0;
		TreeMap<Integer, Integer> tree = new TreeMap<>();
		TreeMap<Integer, Integer> repeatedIDsTree = new TreeMap<>();
		int repeatedIDs = 0;
		for(String csvFilePath : csvFilePaths) {
			System.out.println("Loading CSV data from " + csvFilePath + " ...");
			CSVReader reader = new CSVReader(new FileReader(csvFilePath));
			boolean firstLine = true;
			String line[];
			while((line = reader.readNext()) != null) {
				if(firstLine) {
					firstLine = false;
					testExistenceOfColumnNames(line);
				} else {
					if(Math.abs(ratio - 1.0) > 0.0001 && rand.nextInt(101) >= (100 * ratio)) {
						continue;
					}
					double lat = Double.parseDouble(line[latColumnIndex]);
					double lon = Double.parseDouble(line[lngColumnIndex]);
					String text = line[textColumnIndex];
					String category = line[categoryColumnIndex];
					String url = line[urlColumnIndex];
					Date creationDate = MiningTools.getCreatedAt(line[createdAtColumnIndex], dateFormat);
					String idAsString = line[reportIDColumnIndex];
					Integer id = Integer.parseInt(idAsString);
					
					if(tree.put(id, id) != null) {
						if(repeatedIDsTree.put(id, id) == null)
							repeatedIDs++;
					}
					
					Report rep = new Report(lat, lon, text, category, url, creationDate, null, id);
					reportsCache.put(new Element(id, rep));
					repCount++;
				}
			}
			reader.close();
		}
		int inCacheReports = reportsCache.getKeysWithExpiryCheck().size();
		if(inCacheReports != repCount - repeatedIDs)
			throw new IllegalStateException("Reports: " + repCount + " (repeated: " + repeatedIDs + "), inCache (with expiry check): " + inCacheReports);
		System.out.println("Finished loading CSV data. Total: " + repCount + " reports (repeated: " + repeatedIDs + ")");
	}
	
	private static void loadJSONData(String path, Cache reportsCache, double ratio) throws FileNotFoundException, InterruptedException, ExecutionException {
		File f = new File(path);
		if(!f.isDirectory()) {
			throw new IllegalArgumentException("Path should be to a directory containing the json files");
		}
		
		int repCount = 0;
		TreeMap<Integer, Integer> tree = new TreeMap<>();
		TreeMap<Integer, Integer> repeatedIDsTree = new TreeMap<>();
		int repeatedIDs = 0;
		
		System.out.println("Loading JSON data from " + path + " ...");
		List<Report> reportsList = loadJSONDataFromDirectory(path, ratio);
		for(Report r : reportsList) {
			if(tree.put(r.getID(), r.getID()) != null) {
				if(repeatedIDsTree.put(r.getID(), r.getID()) == null)
					repeatedIDs++;
			}
			reportsCache.put(new Element(r.getID(), r));
			repCount++;
		}
		int inCacheReports = reportsCache.getKeysWithExpiryCheck().size();
		if(inCacheReports != repCount - repeatedIDs)
			throw new IllegalStateException("Reports: " + repCount + " (repeated: " + repeatedIDs + "), inCache (with expiry check): " + inCacheReports);
		System.out.println("Finished loading json data. Total: " + repCount + " reports (repeated: " + repeatedIDs + ")");
	}
	
	private static void testExistenceOfColumnNames(String[] attributeColumns) {
		boolean createdAtExists = false;
		boolean latExists = false;
		boolean lngExists = false;
		boolean categoryExists = false;
		boolean textExists = false;
		boolean urlExists = false;
		boolean idExists = false;
		for(int i = 0; i < attributeColumns.length; i++) {
			String col = attributeColumns[i];
			if(col.equals(createdAtColumn)) {
				createdAtExists = true;
				createdAtColumnIndex = i;
			} else if(col.equals(latColumn)) {
				latExists = true;
				latColumnIndex = i;
			} else if(col.equals(lngColumn)) {
				lngExists = true;
				lngColumnIndex = i;
			} else if(col.equals(categoryColumn)) {
				categoryExists = true;
				categoryColumnIndex = i;
			} else if(col.equals(textColumn)) {
				textExists = true;
				textColumnIndex = i;
			} else if(col.equals(urlColumn)) {
				urlExists = true;
				urlColumnIndex = i;
			} else if(col.equals(reportIDColumn)) {
				idExists = true;
				reportIDColumnIndex = i;
			} 
		}
		if(!createdAtExists) {
			throw new IllegalArgumentException("The specified column " + createdAtColumn + " does not exist in the provided data set.");
		} else if(!latExists) {
			throw new IllegalArgumentException("The specified column " + latColumn + " does not exist in the provided data set.");
		} else if(!lngExists) {
			throw new IllegalArgumentException("The specified column " + lngColumn + " does not exist in the provided data set.");
		} else if(!categoryExists) {
			throw new IllegalArgumentException("The specified column " + categoryColumn + " does not exist in the provided data set.");
		} else if(!textExists) {
			throw new IllegalArgumentException("The specified column " + textColumn + " does not exist in the provided data set.");
		} else if(!urlExists) {
			throw new IllegalArgumentException("The specified column " + urlColumn + " does not exist in the provided data set.");
		} else if(!idExists) {
			throw new IllegalArgumentException("The specified column " + reportIDColumn + " does not exist in the provided data set.");
		}
	}
	
	private static List<Report> loadJSONDataFromDirectory(String directoryPath, double ratio) throws FileNotFoundException {
		File folder = new File(directoryPath);
		File[] listOfFiles = folder.listFiles();
		ArrayList<Report> reports = new ArrayList<Report>();
		
		Random rand = new Random();
		
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getName().contains(".json")) {
				JsonReader jsonReader = new JsonReader(new FileReader(listOfFiles[i]));
				JsonParser jsonParser = new JsonParser();
				JsonArray userarray= jsonParser.parse(jsonReader).getAsJsonArray();
				for (JsonElement jelement : userarray) {
					if(Math.abs(ratio - 1.0) > 0.0001 && rand.nextInt(101) >= (100 * ratio)) {
						continue;
					}
					JsonObject  jobject = jelement.getAsJsonObject();
					Report report = getReportFromJsonObject(jobject);
					reports.add(report);
				}
			}
		}
		
		Collections.sort(reports);
        ArrayList<Report> filtered = new ArrayList<Report>();
        try {
            ArrayList<Report> duplicatesOnly = new ArrayList<Report>();
            for (Report report : reports) {
                if (report.isDuplicate())
                    duplicatesOnly.add(report);
            }

            Date minDate = duplicatesOnly.get(0).getCreationDate();

            for (Report report : reports) {
                if (report.getCreationDate().getTime()>=minDate.getTime())
                    filtered.add(report);
            }
        } catch (Exception e) {
            filtered=reports;
        }



		return filtered;
		
	}

	private static Report getReportFromJsonObject(JsonObject jobject) {
		double lat = new Double(jobject.get("lat")+"");
		double lon = new Double(jobject.get("lng")+"");
		String category = jobject.get("summary")+"";
		String text = jobject.get("description")+"";
		String url = jobject.get("html_url")+"";
		String created_at = jobject.get("created_at")+"";
		
		if (category==null || text==null || url==null) {
			System.out.println(category);
			System.out.println(text);
			System.out.println(url);
			System.exit(0);
		}
		
		Date date = null;
		try {
			date = getCreatedAt_JSON(created_at);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Integer idd = new Integer(jobject.get("id")+"");
		Integer uid = new Integer(jobject.get("reporter").getAsJsonObject().get("id")+"");
		Report report = new Report(lat, lon, text, category, url, date, idd, uid);
		return report;
	}

	private static Date getCreatedAt_JSON(String createdAt) throws ParseException {
		int zoneIndex = createdAt.lastIndexOf("-");
		createdAt = createdAt.substring(0, zoneIndex);
		createdAt = createdAt.replace("\"", "");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date result =  df.parse(createdAt);  	
		return result;
	}
}
