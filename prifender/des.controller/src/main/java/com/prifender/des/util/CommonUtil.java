package com.prifender.des.util;

import java.io.IOException;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.json.simple.JSONObject;

import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataSource;
import com.prifender.messaging.api.MessagingQueue;

public class CommonUtil {
	
	public static String getScheduleQueueName() {
		   
	    final String scheduleQueueName = System.getProperty( "SCHEDULE_QUEUE_NAME" );
	 
		if (scheduleQueueName != null) {
			try {
				return scheduleQueueName;
			} catch (final NumberFormatException e) {
			}
		}
		return "DES-Schedule_Queue";
	}
	public static int getSampleSize(int totalRecords) {
		if(totalRecords > 0 && totalRecords <= 100000){
			return 10000;
		}else if(totalRecords > 100000 && totalRecords <= 200000){
			return 20000;
		}else if(totalRecords > 200000 && totalRecords <= 300000){
			return 30000;
		}else if(totalRecords > 300000 && totalRecords <= 400000){
			return 40000;
		}else if(totalRecords > 400000 && totalRecords <= 500000){
			return 50000;
		}else if(totalRecords > 500000 && totalRecords <= 600000){
			return 60000;
		}else if(totalRecords > 600000 && totalRecords <= 700000){
			return 70000;
		}else if(totalRecords > 700000 && totalRecords <= 800000){
			return 80000;
		}else if(totalRecords > 800000 && totalRecords <= 900000){
			return 900000;
		}else if(totalRecords > 900000 && totalRecords <= 1000000){
			return 100000;
		}else if(totalRecords > 1000000 ){
			return 100000;
		} 
		return 0;
	}
	public static String removeLastChar(String str) {
		return str.substring(0, str.length() - 1);
	}

	public static String getConnectionParam(final DataSource ds, final String param) {
		if (ds == null) {
			throw new IllegalArgumentException();
		}

		if (param == null) {
			throw new IllegalArgumentException();
		}

		for (final ConnectionParam cp : ds.getConnectionParams()) {
			if (cp.getId().equals(param)) {
				return cp.getValue();
			}
		}

		return null;
	}
	public static JsonObjectBuilder jsonObjectBuilder(JSONObject contextParams,String jobName ,
			String jobId,String dependencyJar, String outputmessagingqueue ,String typeId,int chunkSize,int chunkNumber){
		final JsonObjectBuilder chunk = Json.createObjectBuilder();
		chunk.add("chunkId", jobId+"_Chunk_"+chunkNumber);
	    chunk.add("jobName", jobName);
		chunk.add("dependencyJar", dependencyJar);
		chunk.add("contextParams", contextParams.toJSONString());
		chunk.add("objectsExtracted", 0);
		chunk.add("jobId", jobId);
		chunk.add("typeId", typeId);
		chunk.add("state", DataExtractionJob.StateEnum.WAITING.toString());
		chunk.add("numberOfFailedAttempts", 0);
		chunk.add("lastFailureMessage", "");
		chunk.add("timeStarted", "");
		chunk.add("timeCompleted", "");
		chunk.add("nodeId", "");
		chunk.add("chunkSize", chunkSize);
		return chunk;
	}
	
	@SuppressWarnings("unchecked")
	public static void postMessage(MessagingQueue queue,Map<String, String> contextParams,
			String jobName,String jobId,String dependencyJar,String scheduleQueueName,String typeId,
			int calculatedChunkSize,int chunkNumber) throws IOException{
		 JSONObject contextParamsJsonObj = new JSONObject();
		 contextParamsJsonObj.putAll( contextParams );
	     JsonObjectBuilder chunk = jsonObjectBuilder(contextParamsJsonObj,jobName,jobId,dependencyJar,scheduleQueueName,typeId,calculatedChunkSize,chunkNumber);
	     queue.post( chunk.build().toString() );
	}
	
}
