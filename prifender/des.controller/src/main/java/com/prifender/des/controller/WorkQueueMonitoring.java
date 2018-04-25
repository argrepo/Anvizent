package com.prifender.des.controller;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.model.DataExtractionTaskResults;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component
public class WorkQueueMonitoring implements Runnable
{

	@Value( "${scheduling.taskStatusQueue}" )
    private String taskStatusQueueName;
	
	private MessagingQueue taskStatusQueue;
	
	@Autowired
	private MessagingConnectionFactory messaging;
	
	List< DataExtractionTaskResults > dataExtractionTaskResultsList = new LinkedList< DataExtractionTaskResults >( );

	@Override
	public void run()
	{

		try( final MessagingConnection mc = this.messaging.connect() )
		{
		    this.taskStatusQueue = mc.queue(this.taskStatusQueueName);

			this.taskStatusQueue.consume( new MessageConsumer( )
			{

				@Override
				public void consume(final byte[] message)
				{

					final String msg;

					try
					{

						msg = new String( message , "UTF-8" );

						if ( msg != null && msg != "" )
						{

							getTaskStatusQueueResults( msg );

						} else
						{

							System.out.println( "No messages found in " + taskStatusQueueName + "queue" );

						}

					} catch ( InterruptedException e )
					{

						e.printStackTrace( );

					} catch ( Exception e )
					{

						e.printStackTrace( );
					}
				}
			} );
			
		}
		catch ( final IOException e )
		{
			 e.printStackTrace( );
			 
		} finally
		{
			 
			    this.taskStatusQueue = null;

		}
	}

	private void getTaskStatusQueueResults(String msg) throws Exception
	{

		ObjectMapper mapper = new ObjectMapper( );

		DataExtractionTaskResults dataExtractionTaskResults = mapper.readValue( msg , new TypeReference< DataExtractionTaskResults >( ) { } );
		
		int index = getTaskIndexId(dataExtractionTaskResults);
		
		if (index == -1) 
		{
			dataExtractionTaskResultsList.add( dataExtractionTaskResults );
		} 
		else 
		{
			dataExtractionTaskResultsList.set(index, dataExtractionTaskResults);
		}
		
	}
	
	private int getTaskIndexId(DataExtractionTaskResults dataExtractionTaskResults) {
		
		for (int t = 0; t < dataExtractionTaskResultsList.size(); t++) 
		{
			if (dataExtractionTaskResultsList.get(t).getTaskId().equals(dataExtractionTaskResults.getTaskId())) 
			{
				return t;
			}
		}
		return -1;
	}
	

	protected List< DataExtractionTaskResults > getDataExtractionTaskResults(DataExtractionJob  dataExtractionJob)
	{
		List< DataExtractionTaskResults > dataExtractionTaskList = new LinkedList< DataExtractionTaskResults >( );

		if ( dataExtractionTaskResultsList != null && dataExtractionTaskResultsList.size( ) > 0 )
		{
			for ( DataExtractionTaskResults dataExtractionTaskResults : dataExtractionTaskResultsList )
			{
				if ( dataExtractionTaskResults.getJobId( ).equals( dataExtractionJob.getId( ) ) )
				{
					dataExtractionTaskList.add( dataExtractionTaskResults );
					 
				}
			}
		}
		return dataExtractionTaskList;
	}

}
