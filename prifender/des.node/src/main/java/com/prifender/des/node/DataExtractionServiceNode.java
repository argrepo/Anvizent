package com.prifender.des.node;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataExtractionTaskResults;
import com.prifender.encryption.api.Encryption;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component
public class DataExtractionServiceNode implements Runnable {
	
	private static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);
	
	@Value( "${scheduling.pendingTasksQueue}" )
	private String pendingTasksQueueName;
	
	private MessagingQueue pendingTasksQueue;
	
    @Value( "${scheduling.taskStatusQueue}" )
    private String taskStatusQueueName;
    
    private MessagingQueue taskStatusQueue;
    
    @Value( "${scheduling.retries}" )
    private int retries;
    
    @Value( "${talend.jobs}" )
    private String talendJobsPath;
    
	@Autowired
	private Encryption  encryption;
	
	@Autowired
	private MessagingConnectionFactory messagingConnectionFactory;
  
	@Override
	public void run() 
	{
		try( final MessagingConnection mc = this.messagingConnectionFactory.connect() )
		{
		    this.pendingTasksQueue = mc.queue(this.pendingTasksQueueName);
		    
		    this.taskStatusQueue = mc.queue(this.taskStatusQueueName);
		    
		    this.pendingTasksQueue.consume(new MessageConsumer() {
		    	
				public void consume(final byte[] message) {
					
					String msg = null;
					
					try {
						
						msg = new String(message, "UTF-8");
						
						processPendingTask( msg );
						
					 }
					 catch( final UnsupportedEncodingException e )
                       {
						 
                         e.printStackTrace();
                         
                         return;
                         
                      }
					 catch (Exception e) 
					 {
						 try 
						 {
							 
							 retryToPendingTasksQueuOrTaskStatusQueue(msg,e.getMessage());
							 
						} 
						 catch (Exception e1) 
						 {
							e1.printStackTrace();
						}
					 }
				}
			});
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
		    this.pendingTasksQueue = null;
		    this.taskStatusQueue = null;
		}
	}
 
	/**
	 * @param msg
	 * @throws Exception
	 * @throws SQLException
	 */
	private void processPendingTask(String msg) throws Exception, SQLException {
		
		try
		{
		 
		ObjectMapper mapper = new ObjectMapper();
		
		DataExtractionTask dataExtractionJobTask = mapper.readValue(msg, new TypeReference<DataExtractionTask>() { });
		
		if (dataExtractionJobTask != null) 
		{
			String startTime = DATE_FORMAT.format(new Date());
			
			String typeId = null;
			
			String jobName = null;
			
			String dependencyJar = null;
			
			
			String[] typeIdJobIdDependencyJar = dataExtractionJobTask.getTypeId().split( "__" );
			
			if ( typeIdJobIdDependencyJar.length == 3 )
			{
				
				typeId = typeIdJobIdDependencyJar[0];
				
				jobName = typeIdJobIdDependencyJar[1];
				
				dependencyJar = typeIdJobIdDependencyJar[2];
				
			}
			
			dataExtractionJobTask.setContextParameters(decryptContextParams( dataExtractionJobTask.getContextParameters() ));
			
			postTaskToTaskStatusQueue
			 (
					
		         new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())
		
				.jobId(dataExtractionJobTask.getJobId()) 
				
				.timeStarted(startTime) 
				
				.offset(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("OFFSET")))
				
				.limit(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("LIMIT")))
				
				.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())
				
				.lastFailureMessage(dataExtractionJobTask.getLastFailureMessage())
				
				.objectsExtracted(0) , taskStatusQueue
				
			 ); 
			
			DataExtractionETLJobExecution dataExtractionETLJobExecution = new DataExtractionETLJobExecution(this.talendJobsPath, typeId);
            dataExtractionETLJobExecution.runETLjar(jobName,dependencyJar,dataExtractionJobTask.getContextParameters());
			 
			String endTime = DATE_FORMAT.format(new Date());
			
			postTaskToTaskStatusQueue
			 (
					
		         new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())
		
				.jobId(dataExtractionJobTask.getJobId()) 
				
				.timeStarted(startTime).timeCompleted(endTime)
				
				.offset(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("OFFSET")))
				
				.limit(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("LIMIT")))
				
				.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())
				
				.lastFailureMessage(dataExtractionJobTask.getLastFailureMessage())
				
				.objectsExtracted(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("SAMPLESIZE"))),
				
				taskStatusQueue
				
			 );
			
			}
		}catch(Exception e){
			
			throw new Exception( e.getMessage( ) );
			
		} 
	}

	/**
	 * @param dataExtractionTask
	 * @param queue
	 * @throws IOException
	 */
	private void retryTaskToPendingTaskQueue(final DataExtractionTask dataExtractionTask, final MessagingQueue queue) throws IOException 
	{
		Gson gsonObj = new Gson();
		
		String jsonStr = gsonObj.toJson(dataExtractionTask);
		
		queue.post(jsonStr);
	}

	/**
	 * @param dataExtractionTaskResults
	 * @param queue
	 * @throws IOException
	 */
	private void postTaskToTaskStatusQueue(final DataExtractionTaskResults dataExtractionTaskResults, final MessagingQueue queue) throws IOException {
		
		Gson gsonObj = new Gson();
		
		String jsonStr = gsonObj.toJson(dataExtractionTaskResults);
		
		queue.post(jsonStr);
	}

	/**
	 * @param msg
	 * @param lastFailureMessage
	 * @throws Exception
	 */
	private void retryToPendingTasksQueuOrTaskStatusQueue(String msg, String lastFailureMessage) throws Exception 
	{
		try
		{
		
		ObjectMapper mapper = new ObjectMapper();
		
		DataExtractionTask dataExtractionJobTask = mapper.readValue(msg, new TypeReference<DataExtractionTask>() { });
		
		if (dataExtractionJobTask != null) 
		{
			int numberOfFailedAttempts = dataExtractionJobTask.getNumberOfFailedAttempts();
			
			if (numberOfFailedAttempts ==  retries  ) 
			{
				if (msg != null) 
				{
					dataExtractionJobTask.setContextParameters(decryptContextParams( dataExtractionJobTask.getContextParameters() ));
					
					postTaskToTaskStatusQueue(
							
							        new DataExtractionTaskResults().taskId(dataExtractionJobTask.getTaskId())
							
									.jobId(dataExtractionJobTask.getJobId())
									
									.offset(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("OFFSET")))
									
									.limit(Integer.valueOf(dataExtractionJobTask.getContextParameters().get("LIMIT")))
									
									.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts())
									
									.objectsExtracted( 0 )
									
									.lastFailureMessage(lastFailureMessage),
									
									taskStatusQueue);
					
				}
			} else {
				
					dataExtractionJobTask.numberOfFailedAttempts(dataExtractionJobTask.getNumberOfFailedAttempts() + 1)
					
					                      .lastFailureMessage(lastFailureMessage)
					                      
					                      .setContextParameters(dataExtractionJobTask.getContextParameters());
					                     
					 retryTaskToPendingTaskQueue(dataExtractionJobTask, pendingTasksQueue);
					 
					}
				 
			}
		}catch(Exception e)
		{
			throw new Exception( e.getMessage( ) );
		} 
	}
	
	/**
	 * @param encryptedContextParams
	 * @return
	 * @throws Exception
	 */
	private Map<String,String> decryptContextParams( final Map<String , String > encryptedContextParams ) throws Exception
	{
		final Map<String, String> contextParmas = new HashMap<String, String>();
		
		final String decrypted = this.encryption.decrypt(encryptedContextParams.get("encryptedContextParams"));
		
		final JsonArray jsonArray = (JsonArray) new JsonParser().parse( new StringReader(decrypted) );
		 
        for( final JsonElement entry : jsonArray )
        {
            final JsonObject jsonObject = (JsonObject) entry;
            
    		for (final Map.Entry<String, JsonElement> param : jsonObject.entrySet()) 
    		{
    			
    			contextParmas.put(param.getKey(), param.getValue().getAsString());
    			
    		}
          
        }
		
		return contextParmas;
	}
}
