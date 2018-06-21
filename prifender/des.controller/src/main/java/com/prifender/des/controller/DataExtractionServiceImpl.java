package com.prifender.des.controller;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import com.prifender.des.DataExtractionService;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataSourceAdapter.StartResult;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionJob.StateEnum;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataExtractionTaskResults;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;
import com.prifender.encryption.api.Encryption;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component( "dataExtractionService" )
public final class DataExtractionServiceImpl extends DataExtractionService
{
	@Value( "${des.home}" )
	private String desHome;

	@Value( "${des.metadata}" )
	private String desMetadataFile;

	@Value( "${scheduling.pendingTasksQueue}" )
	private String pendingTasksQueueName;

	@Value( "${des.containers}" )
	private int containersCount;

	private MessagingQueue pendingTasksQueue;

	@Autowired
	private Encryption encryption;

	@Autowired
	private List<DataSourceAdapter> adapters;

	@Autowired
	WorkQueueMonitoring workQueueMonitoring;

	@Autowired
	private MessagingConnectionFactory messaging;

	protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);

	private final Map<DataExtractionJob,List<DataExtractionTask>> jobToTasksMap = new IdentityHashMap<>();

	@Override
	public List<DataSourceType> getSupportedDataSourceTypes()
	{
		final List<DataSourceType> types = new ArrayList<DataSourceType>( this.adapters.size() );

		this.adapters.forEach( adapter -> types.add( adapter.getDataSourceType() ) );

		return types;
	}

	private DataSourceAdapter getAdapter( final String type )
	{
		return this.adapters.stream()
				.filter( adapter -> adapter.getDataSourceType().getId().equals( type ) )
				.findFirst()
				.get();
	}

	@Override
	protected ConnectionStatus testConnection( final DataSource ds ) throws DataExtractionServiceException
	{
		return getAdapter( ds.getType() ).testConnection( ds );
	}

	@Override
	protected Metadata getMetadata( final DataSource ds ) throws DataExtractionServiceException
	{
		return getAdapter( ds.getType() ).getMetadata( ds );
	}

	@Override
	protected synchronized DataExtractionJob startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec ) throws DataExtractionServiceException
	{
		final DataSourceAdapter adapter = getAdapter( ds.getType() );

		final StartResult result = adapter.startDataExtractionJob( ds, spec , containersCount);

		if(result.job.getObjectCount() == 0 && result.dataExtractionTasks  == null)
		{
			createOutputMessagingQueue( ds,result.job,spec );	
		}
		else
		{
			postTasksToPendingTasksQueue(result.dataExtractionTasks);
		}
		this.jobToTasksMap.put( result.job, result.dataExtractionTasks);

		return result.job;
	}
	
	private void createOutputMessagingQueue( final DataSource ds, final DataExtractionJob job, final DataExtractionSpec spec ) throws DataExtractionServiceException
	{
		try(MessagingConnection mc = this.messaging.connect( ))
		{
			mc.queue(job.getOutputMessagingQueue());

			synchronized (job) 
			{
				job.setState(DataExtractionJob.StateEnum.SUCCEEDED);
				job.setTimeCompleted(DateFormat.getTimeInstance( DateFormat.DEFAULT ).format(new Date()));
				job.setObjectsExtracted(0);
			}
		}
		catch( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		} 
	}

	@Override
	protected synchronized void deleteDataExtractionJob( final DataExtractionJob job ) throws DataExtractionServiceException
	{
		if( job != null )
		{
			this.jobToTasksMap.remove( job );
		}
	}
	public static boolean isValidTable( Metadata metadata,final String tableName )
	{
		for( final NamedType table : metadata.getObjects() )
		{
			if( table.getName().equals( tableName ) )
			{
				return true;
			}
		}

		return false;
	}

	public static boolean isValidColumn(Metadata metadata, final String tableName, final String columnName )
	{
		for( final NamedType table : metadata.getObjects() )
		{
			if( table.getName().equals( tableName ) )
			{
				final Type entryType = table.getType().getEntryType();

				for( final NamedType column : entryType.getAttributes() )
				{
					if( column.getName().equals( columnName ) )
					{
						return true;
					}
				}
			}
		}

		return false;
	}

	@Override
	protected List<DataSource> loadDataSources()
	{
		final List<DataSource> dataSources = new ArrayList<>();
		final File metadataFile = new File( this.desMetadataFile );

		if( metadataFile.exists() )
		{
			try( final Reader reader = new FileReader( metadataFile ) )
			{
				final JsonArray jsonArray = (JsonArray) new JsonParser().parse( reader );

				for( final JsonElement entry : jsonArray )
				{
					final JsonObject jsonObject = (JsonObject) entry;

					final DataSource ds = new DataSource()
							.id( jsonObject.get( "id" ).getAsString() )
							.label( jsonObject.get( "label" ).getAsString() )
							.type( jsonObject.get( "type" ).getAsString() );

					final JsonElement description = jsonObject.get( "description" );

					if( description != null )
					{
						ds.description( description.getAsString() );
					}

					decryptConnectionParams( jsonObject.get( "connectionParams" ).getAsString(), ds );

					dataSources.add( ds );
				}
			}
			catch( final Exception e )
			{
				// If couldn't read the metadata or it was malformed, log error and continue

				e.printStackTrace();
			}
		}

		return dataSources;
	}

	private void saveDataSources()
	{
		try( final JsonWriter writer = new JsonWriter( new FileWriter( this.desMetadataFile ) ) )
		{
			writer.setIndent( "    " );
			writer.setSerializeNulls( false );
			writer.beginArray();

			for( final DataSource ds : getDataSources() )
			{
				writer.beginObject();

				writer.name( "id" ).value( ds.getId() );
				writer.name( "label" ).value( ds.getLabel() );
				writer.name( "type" ).value( ds.getType() );
				writer.name( "description" ).value( ds.getDescription() );
				writer.name( "connectionParams" ).value( encryptConnectionParams( ds.getConnectionParams() ) );

				writer.endObject();
			}

			writer.endArray();
		}
		catch( final Exception e )
		{
			e.printStackTrace();
		}
	}

	private String encryptConnectionParams( final List<ConnectionParam> params ) throws Exception
	{
		final StringWriter sw = new StringWriter();

		try( final JsonWriter writer = new JsonWriter( sw ) )
		{
			writer.beginObject();

			for( final ConnectionParam param : params )
			{
				writer.name( param.getId() );
				writer.value( param.getValue() );
			}

			writer.endObject();
		}

		return this.encryption.encrypt( sw.toString() );
	}

	private void decryptConnectionParams( final String encrypted, final DataSource ds ) throws Exception
	{
		final String decrypted = this.encryption.decrypt( encrypted );
		final JsonObject connectionParamsParsed = (JsonObject) new JsonParser().parse( new StringReader( decrypted ) );

		for( final Map.Entry<String,JsonElement> param : connectionParamsParsed.entrySet() )
		{
			ds.addConnectionParamsItem( new ConnectionParam().id( param.getKey() ).value( param.getValue().getAsString() ) );
		}
	}

	@Override
	public synchronized DataSource addDataSource( final DataSource ds ) throws DataExtractionServiceException
	{
		final DataSource result = super.addDataSource( ds );
		saveDataSources();
		return result;
	}

	@Override
	public synchronized void updateDataSource( final String id, final DataSource ds ) throws DataExtractionServiceException
	{
		super.updateDataSource( id, ds );
		saveDataSources();
	}

	@Override
	public synchronized void deleteDataSource( final String id )
	{
		super.deleteDataSource( id );
		saveDataSources();
	}


	@Override
	public synchronized List<DataExtractionJob> getDataExtractionJobs() {

		List<DataExtractionJob> dataExtractionJobList = new ArrayList<DataExtractionJob>();

		List<DataExtractionJob> dataExtractionJobs =  super.getDataExtractionJobs();

		for(DataExtractionJob dataExtractionJob : dataExtractionJobs){

			dataExtractionJobList.add(getDataExtractionJob(dataExtractionJob.getId()));
		}

		return dataExtractionJobList;
	}

	@Override
	public synchronized DataExtractionJob getDataExtractionJob(String id)
	{

		DataExtractionJob  dataExtractionJob = super.getDataExtractionJob( id );

		List< DataExtractionTaskResults > dataExtractionTaskResults = workQueueMonitoring.getDataExtractionTaskResults( dataExtractionJob );

		if ( dataExtractionTaskResults.size( ) > 0 )
		{
			int chunksCount = dataExtractionTaskResults.size( );

			int objectsExtracted = 0 ,failureMessageCount = 0, count = 1;

			String timeStarted = null , timeCompleted = null , lastFailureMessage=null;

			List< DataExtractionTaskResults > dataExtractionChunks = new ArrayList< DataExtractionTaskResults >( );

			for ( DataExtractionTaskResults dataExtractionChunk : dataExtractionTaskResults )
			{
				if ( dataExtractionChunk.getJobId( ).equals( dataExtractionJob.getId( ) ) )
				{
					if ( count == 1 )

						timeStarted = dataExtractionChunk.getTimeStarted( );

					if ( count == chunksCount )

						timeCompleted = dataExtractionChunk.getTimeCompleted( );

					dataExtractionChunks.add( dataExtractionChunk );

					objectsExtracted +=  dataExtractionChunk.getObjectsExtracted( ) ;

					lastFailureMessage = dataExtractionChunk.getLastFailureMessage( );

					if ( lastFailureMessage != null )
					{
						failureMessageCount++ ;
					}

					count++ ;
				}
			}
			dataExtractionJob.setObjectsExtracted( objectsExtracted );

			if ( failureMessageCount > 0 )
			{
				dataExtractionJob.setState( StateEnum.FAILED );

			} else
			{
				if ( dataExtractionJob.getTasksCount( ) == chunksCount )
				{
					dataExtractionJob.setState( StateEnum.SUCCEEDED );
				}
				else
				{
					dataExtractionJob.setState( StateEnum.RUNNING );
				}
			}
			dataExtractionJob.setTimeStarted( timeStarted );

			dataExtractionJob.setTimeCompleted( timeCompleted );

			dataExtractionJob.setDataExtractionTasks( dataExtractionChunks );
		}
		return dataExtractionJob;
	} 
	private void postTasksToPendingTasksQueue(List<DataExtractionTask> dataExtractionTasks) throws DataExtractionServiceException
	{

		try(MessagingConnection mc = messaging.connect( ))
		{

			this.pendingTasksQueue = mc.queue(this.pendingTasksQueueName);

			for(DataExtractionTask dataExtractionTask : dataExtractionTasks){

				dataExtractionTask.setContextParameters(encryptContextParams( dataExtractionTask.getContextParameters() ));

				postPendingTask(dataExtractionTask, pendingTasksQueue);

			}
		}
		catch( Exception  e ){

			throw new DataExtractionServiceException(new Problem().code("schedule queue error").message(e.getMessage()));

		} 
	}

	private  void postPendingTask(final DataExtractionTask dataExtractionJobTask, final MessagingQueue queue) throws IOException
	{
		Gson gsonObj = new Gson();

		String jsonStr = gsonObj.toJson(dataExtractionJobTask);

		queue.post(jsonStr);

	}

	private Map<String,String> encryptContextParams( final Map<String , String > contextParams ) throws Exception
	{
		final Map<String, String> contextParmas = new HashMap<String, String>();

		StringWriter sw = new StringWriter();

		try (final JsonWriter writer = new JsonWriter(sw)) 
		{
			writer.setIndent("    ");
			writer.setSerializeNulls(false);
			writer.beginArray();

			contextParams.entrySet().forEach(entry -> 
			{
				try 
				{
					writer.beginObject();
					writer.name(entry.getKey());
					writer.value(entry.getValue());
					writer.endObject();
				} 

				catch (Exception e) 
				{
					throw new RuntimeException(e);
				}

			});

			writer.endArray();
		}

		contextParmas.put("encryptedContextParams", this.encryption.encrypt(sw.toString()));

		return contextParmas;
	}
}
