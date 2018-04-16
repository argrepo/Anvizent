package com.prifender.des.controller;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.model.DataExtractionChunk;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.Problem;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component
public class WorkQueueMonitoring {
	@Autowired
	private MessagingConnectionFactory  messaging;
    Queue<DataExtractionChunk> dataExtractionChunkQueue = new LinkedList<DataExtractionChunk>();
    @Scheduled(fixedRate = 2000)
    public void create() throws DataExtractionServiceException {
    	try( final MessagingConnection messagingServiceConnection = this.messaging.connect() ){
         {
        	 final String queueName = getResultsScheduleQueueName();
        	 final MessagingQueue queue = messagingServiceConnection.queue( queueName );
             
             queue.consume
             (
                 new MessageConsumer()
                 {
					@Override
                     public void consume( final byte[] message )
                     {
                          final String msg;
                          try
                          {
                              msg = new String( message, "UTF-8" );
                              if(msg != null && msg != ""){
							  doWork(msg);
                              }else{
                            	 System.out.println("No messages found in "+queueName +"queue");
                              }
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (Exception e) {
								e.printStackTrace();
							}
                     }
                 } 
             );
          }
         }catch( final IOException e )
         {
        	 throw new DataExtractionServiceException(new Problem().code("messaging connection error").message(e.getCause().toString()));
         }
    	}
    private void doWork(String msg) throws Exception{
		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		DataExtractionChunk dataExtractionChunk = mapper.readValue(msg, new TypeReference<DataExtractionChunk>(){});
		dataExtractionChunkQueue.add(dataExtractionChunk);
}
    Queue<DataExtractionChunk> getDataExtractionChunkQueue(DataExtractionJob finaldataExtractionJob){
    	Queue<DataExtractionChunk> dataExtractionJobQueue = new LinkedList<DataExtractionChunk>();
    	if(dataExtractionChunkQueue != null && dataExtractionChunkQueue.size() > 0){
    		for(DataExtractionChunk dataExtractionChunk : dataExtractionChunkQueue){
    			if(dataExtractionChunk.getJobId().equals(finaldataExtractionJob.getId())){
    				dataExtractionJobQueue.add(dataExtractionChunk);
    			}
          }
    	}
	   return dataExtractionJobQueue;
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
}
    	
