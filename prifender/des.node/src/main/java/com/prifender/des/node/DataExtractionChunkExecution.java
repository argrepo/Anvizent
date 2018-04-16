package com.prifender.des.node;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingQueue;
@Component
public class DataExtractionChunkExecution  implements Runnable {
	
	protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);
	@Autowired
	private TalendJobExecution talendJobExecution;
	@Autowired
	private EncryptionServiceClient encryptionServiceClient;
	private static final String PROP_MAX_SLEEP = "MAX_SLEEP";
  
	@Override
	public void run() {
		try {
			final String scheduleQueueName = getScheduleQueueName();
		    final int maxSleep = getMaxSleep();
			GetMessagingQueue getMessagingQueue = new GetMessagingQueue();
			final MessagingQueue queue = getMessagingQueue.getQueue(scheduleQueueName);
			queue.consume(new MessageConsumer() {
				public void consume(final byte[] message) {
					String msg = null;
					try {
						msg = new String(message, "UTF-8");
						if(msg != null ){
						 if(!talendJobExecution.isJobRunning()){
							 doWork(msg); 
						 }
						}
					   if (maxSleep > 0) {
						final int sleep = (int) (Math.random() * maxSleep);
						try {
							Thread.sleep(sleep);
						} catch (final InterruptedException e) {
							e.printStackTrace();
						}
						}   
					 }catch( final UnsupportedEncodingException e )
                       {
                         e.printStackTrace();
                         return;
                      }catch (Exception e) {
						 try {
							postMessageForRetryToWorkQueueAndResultQueue(msg,e.getMessage());
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					}
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		 }
		}

	public String getIpAddressAndPort()
			throws MalformedObjectNameException, NullPointerException, UnknownHostException {
		MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
		Set<ObjectName> objectNames = beanServer.queryNames(new ObjectName("*:type=Connector,*"),
				Query.match(Query.attr("protocol"), Query.value("HTTP/1.1")));
		String host = InetAddress.getLocalHost().getHostAddress();
		String port = objectNames.iterator().next().getKeyProperty("port");
		return host + ":" + port;
	}

	public String getUUID() throws MalformedObjectNameException, NullPointerException, UnknownHostException {
		UUID uuid = UUID.randomUUID();
		String randomUUIDString = uuid.toString();
		return getIpAddressAndPort() + "-" + randomUUIDString;
	}

	private JsonObjectBuilder resultsJsonObjectBuilder(String chunkId, String jobId, String state, String timeStarted,
			String timeCompleted, int offset, int limit, String nodeId, int numberOfFailedAttempts,
			String lastFailureMessage, int objectsExtracted) {
		final JsonObjectBuilder chunk = Json.createObjectBuilder();
		chunk.add("chunkId", chunkId);
		chunk.add("jobId", jobId);
		chunk.add("state", state);
		chunk.add("timeStarted", timeStarted);
		chunk.add("timeCompleted", timeCompleted);
		chunk.add("offset", offset);
		chunk.add("limit", limit);
		chunk.add("nodeId", nodeId);
		chunk.add("numberOfFailedAttempts", numberOfFailedAttempts);
		chunk.add("lastFailureMessage", lastFailureMessage);
		chunk.add("objectsExtracted", objectsExtracted);
		return chunk;
	}
 
	private JsonObjectBuilder retryJsonObjectBuilder(String chunkId, String jobId, String state, String timeStarted,
			String timeCompleted, String jobName, String dependencyJar, JSONObject contextParams, String nodeId, int numberOfFailedAttempts,
			String lastFailureMessage, int objectsExtracted,int chunkSize,String typeId) {
		final JsonObjectBuilder chunk = Json.createObjectBuilder();
		chunk.add("jobName", jobName);
		chunk.add("dependencyJar", chunkId);
		chunk.add("contextParams", contextParams.toJSONString());
		chunk.add("chunkId", chunkId);
		chunk.add("jobId", jobId);
		chunk.add("state", state);
		chunk.add("timeStarted", timeStarted);
		chunk.add("timeCompleted", timeCompleted);
		chunk.add("typeId", typeId);
		chunk.add("nodeId", nodeId);
		chunk.add("numberOfFailedAttempts", numberOfFailedAttempts);
		chunk.add("lastFailureMessage", lastFailureMessage);
		chunk.add("objectsExtracted", objectsExtracted);
		chunk.add("chunkSize", chunkSize);
		return chunk;
	}
	
	private void doWork(String msg) throws Exception ,SQLException {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = new HashMap<String, Object>();
		map = mapper.readValue(msg, new TypeReference<Map<String, Object>>() {
		});
		if (map.size() > 0 && map != null) {
				String jobName = (String) map.get("jobName");
				String chunkId = (String) map.get("chunkId");
				String jobId = (String) map.get("jobId");
				String dependencyJar = (String) map.get("dependencyJar");
				String typeId = (String) map.get("typeId");
				ObjectMapper objectMapper = new ObjectMapper();
				Map<String, String> contextParams = new HashMap<String, String>();
				contextParams = objectMapper.readValue(map.get("contextParams").toString(), new TypeReference<Map<String, String>>() {});
				decryptParams(contextParams);
				String startTime = DATE_FORMAT.format(new Date());
				int chunksSize = Integer.parseInt(contextParams.get("SAMPLESIZE"));
				int offset = Integer.parseInt(contextParams.get("OFFSET"));
				int limit = Integer.parseInt(contextParams.get("LIMIT"));
				Process process = talendJobExecution.runEtlJar(jobName, dependencyJar, contextParams, typeId);
				if (process != null) {
					String endTime = DATE_FORMAT.format(new Date());
					JsonObjectBuilder chunk = resultsJsonObjectBuilder(chunkId, jobId, "succeeded", startTime, endTime, offset,
							limit, getIpAddressAndPort(), 0, "", chunksSize);
					GetMessagingQueue getMessagingQueue = new GetMessagingQueue();
					final String resultsworkQueueName = getResultsScheduleQueueName();
					final MessagingQueue workQueue = getMessagingQueue.getQueue(resultsworkQueueName);
					workQueue.post(chunk.build().toString());
				}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void postMessageForRetryToWorkQueueAndResultQueue(String msg, String lastFailureMessage) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = new HashMap<String, Object>();
		map = mapper.readValue(msg, new TypeReference<Map<String, Object>>() {
		});
		if (map.size() > 0 && map != null) {
			 int numberOfFailedAttempts = (Integer) map.get("numberOfFailedAttempts");
			 if(numberOfFailedAttempts == getThreshold()){
					String chunkId = (String) map.get("chunkId");
					String jobId = (String) map.get("jobId");
					ObjectMapper objectMapper = new ObjectMapper();
					Map<String, String> contextParams = new HashMap<String, String>();
					contextParams = objectMapper.readValue(map.get("contextParams").toString(), new TypeReference<Map<String, String>>() {});
					int offset = Integer.parseInt(contextParams.get("OFFSET"));
					int limit = Integer.parseInt(contextParams.get("LIMIT"));
					if(msg != null){
						JsonObjectBuilder chunk = resultsJsonObjectBuilder(chunkId, jobId, "Failed","", "", offset, limit,getIpAddressAndPort(), numberOfFailedAttempts, lastFailureMessage, 0);
						GetMessagingQueue getMessagingQueue = new GetMessagingQueue();
						final String resultsworkQueueName = getResultsScheduleQueueName();
						final MessagingQueue workQueue = getMessagingQueue.getQueue(resultsworkQueueName);
						workQueue.post(chunk.build().toString());
				}
			 }else{
				 try {
						String jobName = (String) map.get("jobName");
						String chunkId = (String) map.get("chunkId");
						String jobId = (String) map.get("jobId");
						String dependencyJar = (String) map.get("dependencyJar");
						String typeId = (String) map.get("typeId");
						ObjectMapper objectMapper = new ObjectMapper();
						Map<String, String> contextParams = new HashMap<String, String>();
						contextParams = objectMapper.readValue(map.get("contextParams").toString(), new TypeReference<Map<String, String>>() {});
						JSONObject contextParamsJsonObj = new JSONObject();
						contextParamsJsonObj.putAll(contextParams);
						String timeStarted = (String) map.get("timeStarted");
						String timeCompleted = (String) map.get("timeCompleted");
						int chunkSize = (Integer) map.get("chunkSize");
						int objectsExtracted = (Integer) map.get("objectsExtracted");

						JsonObjectBuilder chunk = retryJsonObjectBuilder(chunkId, jobId, "Failed", timeStarted,
								timeCompleted, jobName, dependencyJar, contextParamsJsonObj, getIpAddressAndPort(), numberOfFailedAttempts+1,
								lastFailureMessage, objectsExtracted,chunkSize,typeId);
						if(msg != null ){
							GetMessagingQueue getMessagingQueue = new GetMessagingQueue();
							final String workQueueName = getScheduleQueueName();
							final MessagingQueue workQueue = getMessagingQueue.getQueue(workQueueName);
							workQueue.post(chunk.build().toString());
						}
				} catch (Exception e1) {
					e1.printStackTrace();
				} 
			 }
		}
	}
	private static int getMaxSleep() {
		final String maxSleepStr = System.getenv(PROP_MAX_SLEEP);
		if (maxSleepStr != null) {
			try {
				return Integer.parseInt(maxSleepStr);
			} catch (final NumberFormatException e) {
			}
		}
		return 2000;
	}
	
	private static int getThreshold() {
		   
	    final String threshold = System.getenv( "THRESHOLD" );
	 
		if (threshold != null) {
			try {
				return Integer.parseInt(threshold);
			} catch (final NumberFormatException e) {
			}
		}
		return 3;
	}
	private static String getScheduleQueueName() {
		   
	    final String scheduleQueueName = System.getenv( "SCHEDULE_QUEUE_NAME" );
	 
		if (scheduleQueueName != null) {
			try {
				return scheduleQueueName;
			} catch (final NumberFormatException e) {
			}
		}
		return "DES-Schedule_Queue";
	}
	private static String getResultsScheduleQueueName() {
		   
	    final String resultsScheduleQueueName = System.getenv( "SCHEDULE_RESULTS_QUEUE_NAME" );
	 
		if (resultsScheduleQueueName != null) {
			try {
				return resultsScheduleQueueName;
			} catch (final NumberFormatException e) {
			}
		}
		return "DES-Schedule_Queue_Results";
	}
	
	Map<String,String> decryptParams(Map<String, String> contextParams) throws GeneralSecurityException{
		if(contextParams.get("DATASOURCE_USER") != null && contextParams.get("DATASOURCE_USER") != "")
		contextParams.put("DATASOURCE_USER", encryptionServiceClient.decrypt( contextParams.get("DATASOURCE_USER")));
		if(contextParams.get("DATASOURCE_PASS") != null && contextParams.get("DATASOURCE_PASS") != "")
		contextParams.put("DATASOURCE_PASS", encryptionServiceClient.decrypt( contextParams.get("DATASOURCE_PASS")));
		if(contextParams.get("HADOOP_USER_NAME") != null && contextParams.get("HADOOP_USER_NAME") != "")
	    contextParams.put("HADOOP_USER_NAME", encryptionServiceClient.decrypt( contextParams.get("HADOOP_USER_NAME")));
		
		return contextParams;
	}
}
