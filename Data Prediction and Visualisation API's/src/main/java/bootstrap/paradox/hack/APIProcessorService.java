package bootstrap.paradox.hack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class APIProcessorService {
	MongoClient mongo;
	MongoDatabase database;
	MongoCollection<Document> collection;

	public APIProcessorService() {
		init();
	}

	public void init() {
		mongo = new MongoClient("localhost", 27017);

		database = mongo.getDatabase("config");

		collection = database.getCollection("geo_spatial");
	}

	public Integer getTimeInterval(String lat1, String lon1, String lat2, String lon2, int hrs)
			throws ClientProtocolException, IOException, JSONException {

		String url = "http://osrm-1644136849.us-east-1.elb.amazonaws.com/route/v1/driving/" + lon1 + "," + lat1 + ";"
				+ lon2 + "," + lat2 + "?overview=false&steps=true&annotations=true";

		HttpClient client = HttpClientBuilder.create().build();
		HttpGet request = new HttpGet(url);

		HttpResponse response = client.execute(request);
		HttpEntity entity = response.getEntity();

		String content = EntityUtils.toString(entity);
		JSONObject jsonObject = new JSONObject(content);
		JSONArray routes = jsonObject.getJSONArray("routes");
		JSONArray legs = (JSONArray) ((JSONObject) routes.get(0)).get("legs");

		JSONObject annotation = (JSONObject) ((JSONObject) legs.get(0)).get("annotation");
		JSONArray nodes = (JSONArray) annotation.get("nodes");

		int timeTaken = calculateTime(nodes, hrs);
		return timeTaken;
	}

	public String getVisualisationData(int hours, Date toDate) {

		Map<String, TimeDistance> nodeToAvgTimeMapping = new HashMap<>();

		BasicDBObject whereQuery = new BasicDBObject();

		Calendar cal = Calendar.getInstance();
		// remove next line if you're always using the current time.
		cal.setTime(toDate);
		cal.add(Calendar.HOUR, -1 * hours);
		Date fromDate = cal.getTime();
		// get floor of hour
		BasicDBObject ltQuery = new BasicDBObject();
		ltQuery.put("$lt", toDate);

		ltQuery.put("$gt", fromDate);
		whereQuery.put("date", ltQuery);

		Iterable<Document> cursor = collection.find(whereQuery);

		while (cursor.iterator().hasNext()) {
			Document obj = cursor.iterator().next();
			if (!nodeToAvgTimeMapping.containsKey(obj.getString("node_from") + ":" + obj.getString("node_to"))
					&& !nodeToAvgTimeMapping.containsKey(obj.getString("node_to") + ":" + obj.getString("node_from"))) {
				TimeDistance td = new TimeDistance(obj.getLong("total_time_taken"),
						obj.getInteger("number_of_data_points"));
				String key = obj.getString("node_from") + ":" + obj.getString("node_to");
				nodeToAvgTimeMapping.put(key, td);
			} else {
				TimeDistance td = nodeToAvgTimeMapping.get(obj.getString("node_from") + ":" + obj.getString("node_to"));
				String key = obj.getString("node_from") + ":" + obj.getString("node_to");
				if (td == null) {
					key = obj.getString("node_to") + ":" + obj.getString("node_from");
					td = nodeToAvgTimeMapping.get(key);
					td.time += obj.getLong("total_time_taken");
					td.dataPts += obj.getInteger("number_of_data_points");
				}
			}
		}
		List<Map<String, String>> resultList = new ArrayList<>();
		for (Map.Entry<String, TimeDistance> entry : nodeToAvgTimeMapping.entrySet()) {
			String key = entry.getKey();
			String from_node = key.split(":")[0];
			String to_node = key.split(":")[1];
			long time = entry.getValue().time;
			int dataPts = entry.getValue().dataPts;
			long avg = time / dataPts;
			Map<String, String> res = new HashMap<>();
			res.put("from_node", from_node);
			res.put("to_node", to_node);
			res.put("avgSpeed", String.valueOf(avg));
			res.put("colour", getColour(avg));
			resultList.add(res);
		}
		return new JSONObject((Map) resultList).toString();
	}

	public String getColour(long avg) {
		if (avg <= 30) {
			return "Dark Red(Slow coach)";
		} else if (avg <= 40) {
			return "Light Red(Pick up speed)";
		} else if (avg <= 80) {
			return "Light Green(Keep going)";
		} else {
			return "Dark Green(God save you)";
		}

	}

	int calculateTime(JSONArray nodes, int hrs) throws JSONException {
		int time = 0;
		int len = nodes.length();
		String firstNode = nodes.get(0).toString();
		for (int i = 1; i < len; i++) {
			time += getAverageTime(firstNode, nodes.get(i).toString(), String.valueOf(hrs));
			firstNode = nodes.get(i).toString();
		}
		return time;
	}

	int getAverageTime(String node1, String node2, String hrs) {
		int time = 0;

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("node_from", node1);
		whereQuery.put("node_to", node2);
		// get floor of hour
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		whereQuery.put("time.hour", hrs);

		Iterable<Document> cursor = collection.find(whereQuery);

		long total_time_taken = 0;
		int number_of_data_points = 0;

		while (cursor.iterator().hasNext()) {
			Document obj = cursor.iterator().next();
			total_time_taken += obj.getLong("total_time_taken");
			number_of_data_points += obj.getInteger("number_of_data_points");
		}
		if (number_of_data_points > 0) {
			time = (int) (total_time_taken / number_of_data_points);
		} else {
			whereQuery = new BasicDBObject();
			whereQuery.put("node_from", node1);
			whereQuery.put("node_to", node2);
			cursor = collection.find(whereQuery);

			total_time_taken = 0;
			number_of_data_points = 0;

			while (cursor.iterator().hasNext()) {
				Document obj = cursor.iterator().next();
				total_time_taken = obj.getLong("total_time_taken");
				number_of_data_points = obj.getInteger("number_of_data_points");
			}
			if (number_of_data_points > 0) {
				time = (int) (total_time_taken / number_of_data_points);
			}
		}
		return time;

	}

}

class TimeDistance {
	int dataPts;
	long time;

	public TimeDistance(long time, int dataPts) {
		this.time = time;
		this.dataPts = dataPts;
	}

}
