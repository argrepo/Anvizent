package com.prifender.des.controller;

import static com.prifender.des.util.DatabaseUtil.getMongoDataBaseConnection;
import static com.prifender.des.util.DatabaseUtil.destroy;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.prifender.des.DataExtractionService;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataSourceAdapter.StartResult;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionJob.StateEnum;
import com.prifender.des.model.DataExtractionSpec;
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

@Component("dataExtractionService")
public final class DataExtractionServiceImpl extends DataExtractionService
{
	@Value("${des.home}")
	private String desHome;

	@Value("${des.metadata}")
	private String desMetadataFile;

	@Value("${scheduling.pendingTasksQueue}")
	private String pendingTasksQueueName;

	@Value("${mongo.db.host}")
	private String host;

	@Value("${mongo.db.port}")
	private int port;

	@Value("${mongo.db.userName}")
	private String userName;

	@Value("${mongo.db.password}")
	private String password;

	@Value("${mongo.db.database}")
	private String database;

	@Value("${mongo.db.collection}")
	private String collection;

	@Value("${des.containers}")
	private int containersCount;

	@Autowired
	private Encryption encryption;

	@Autowired
	private List<DataSourceAdapter> adapters;

	@Autowired
	WorkQueueMonitoring workQueueMonitoring;

	@Autowired
	private MessagingConnectionFactory messaging;

	protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance(DateFormat.DEFAULT);

	private final Map<DataExtractionJob, DataExtractionThread> jobToThreadMap = new IdentityHashMap<>();

	@Override
	public List<DataSourceType> getSupportedDataSourceTypes()
	{
		final List<DataSourceType> types = new ArrayList<DataSourceType>(this.adapters.size());

		this.adapters.forEach(adapter -> types.add(adapter.getDataSourceType()));

		return types;
	}

	private DataSourceAdapter getAdapter(final String type)
	{
		return this.adapters.stream().filter(adapter -> adapter.getDataSourceType().getId().equals(type)).findFirst().get();
	}

	@Override
	protected ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException
	{
		return getAdapter(ds.getType()).testConnection(ds);
	}

	@Override
	protected Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		return getAdapter(ds.getType()).getMetadata(ds);
	}

	@Override
	protected synchronized DataExtractionJob startDataExtractionJob(final DataSource ds, final DataExtractionSpec spec) throws DataExtractionServiceException
	{
		final DataSourceAdapter adapter = getAdapter(ds.getType());

		final StartResult result = adapter.startDataExtractionJob(ds, spec, containersCount);

		this.jobToThreadMap.put(result.job, result.thread);

		return result.job;
	}

	@Override
	protected synchronized void deleteDataExtractionJob(final DataExtractionJob job) throws DataExtractionServiceException
	{
		final DataExtractionThread thread = (DataExtractionThread) this.jobToThreadMap.remove(job);

		if( thread != null )
		{
			thread.cancel();
		}
		deleteOutputMessagingQueue(job);
	}

	public static boolean isValidTable(Metadata metadata, final String tableName)
	{
		for (final NamedType table : metadata.getObjects())
		{
			if( table.getName().equals(tableName) )
			{
				return true;
			}
		}

		return false;
	}

	public static boolean isValidColumn(Metadata metadata, final String tableName, final String columnName)
	{
		for (final NamedType table : metadata.getObjects())
		{
			if( table.getName().equals(tableName) )
			{
				final Type entryType = table.getType().getEntryType();

				for (final NamedType column : entryType.getAttributes())
				{
					if( column.getName().equals(columnName) )
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
		final File metadataFile = new File(this.desMetadataFile);

		if( metadataFile.exists() )
		{
			try ( final Reader reader = new FileReader(metadataFile))
			{
				final JsonArray jsonArray = (JsonArray) new JsonParser().parse(reader);

				for (final JsonElement entry : jsonArray)
				{
					final JsonObject jsonObject = (JsonObject) entry;

					final DataSource ds = new DataSource().id(jsonObject.get("id").getAsString()).label(jsonObject.get("label").getAsString()).type(jsonObject.get("type").getAsString());

					final JsonElement description = jsonObject.get("description");

					if( description != null )
					{
						ds.description(description.getAsString());
					}

					decryptConnectionParams(jsonObject.get("connectionParams").getAsString(), ds);

					dataSources.add(ds);
				}
			}
			catch ( final Exception e )
			{
				// If couldn't read the metadata or it was malformed, log error
				// and continue

				e.printStackTrace();
			}
		}

		return dataSources;
	}

	/*@Override
	protected List<DataSource> loadDataSources()
	{
		final List<DataSource> dataSources = new ArrayList<>();
		MongoClient mongoClient = null;
		MongoCollection<Document> mongoCollection = null;
		List<Document> documents = null;
		try
		{
			mongoClient = getMongoDataBaseConnection(host, port, database, userName, password);
			if( mongoClient != null )
			{
				MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
				mongoCollection = mongoDatabase.getCollection(collection);
				documents = (List<Document>) mongoCollection.find().into(new ArrayList<Document>());
				for (Document document : documents)
				{
					final DataSource ds = new DataSource().id(document.get("id").toString()).label(document.get("label").toString()).type(document.get("type").toString());

					final String description = document.get("description").toString();

					if( description != null )
					{
						ds.description(document.get("description").toString());
					}

					decryptConnectionParams(document.get("connectionParams").toString(), ds);

					dataSources.add(ds);
				}

			}
		}
		catch ( final Exception e )
		{
			e.printStackTrace();
		}
		finally
		{
			destroy(mongoClient);
		}
		return dataSources;
	}*/

	private void saveDataSources()
	{
		try ( final JsonWriter writer = new JsonWriter(new FileWriter(this.desMetadataFile)))
		{
			writer.setIndent("    ");
			writer.setSerializeNulls(false);
			writer.beginArray();

			for (final DataSource ds : getDataSources())
			{
				writer.beginObject();

				writer.name("id").value(ds.getId());
				writer.name("label").value(ds.getLabel());
				writer.name("type").value(ds.getType());
				writer.name("description").value(ds.getDescription());
				writer.name("connectionParams").value(encryptConnectionParams(ds.getConnectionParams()));

				writer.endObject();
			}

			writer.endArray();
		}
		catch ( final Exception e )
		{
			e.printStackTrace();
		}
	}

	private void saveDataSourcesToMongoDatabase() throws DataExtractionServiceException
	{
		List<String> collections = new ArrayList<>();
		boolean collectionExist = false;
		MongoClient mongoClient = null;
		MongoCursor<Document> documents = null;
		try
		{
			mongoClient = getMongoDataBaseConnection(host, port, database, userName, "SO#h3TbY*hx8ypJP");
			if( mongoClient != null )
			{
				MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
				documents = mongoDatabase.listCollections().iterator();
				while (documents.hasNext())
				{
					collections.add(documents.next().get("name").toString());
				}
				for (String col : collections)
				{
					if( col.equals(collection) )
					{
						collectionExist = true;
					}
				}

				if( collectionExist )
				{
					createDocument(mongoDatabase.getCollection(collection));
				}
				else
				{
					mongoDatabase.createCollection(collection);
					createDocument(mongoDatabase.getCollection(collection));
				}
			}
		}
		catch ( final Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(mongoClient, documents);
		}
	}

	private String encryptConnectionParams(final List<ConnectionParam> params) throws Exception
	{
		final StringWriter sw = new StringWriter();

		try ( final JsonWriter writer = new JsonWriter(sw))
		{
			writer.beginObject();

			for (final ConnectionParam param : params)
			{
				writer.name(param.getId());
				writer.value(param.getValue());
			}

			writer.endObject();
		}

		return this.encryption.encrypt(sw.toString());
	}

	private void decryptConnectionParams(final String encrypted, final DataSource ds) throws Exception
	{
		final String decrypted = this.encryption.decrypt(encrypted);
		final JsonObject connectionParamsParsed = (JsonObject) new JsonParser().parse(new StringReader(decrypted));

		for (final Map.Entry<String, JsonElement> param : connectionParamsParsed.entrySet())
		{
			ds.addConnectionParamsItem(new ConnectionParam().id(param.getKey()).value(param.getValue().getAsString()));
		}
	}

	@Override
	public synchronized DataSource addDataSource(final DataSource ds) throws DataExtractionServiceException
	{
		final DataSource result = super.addDataSource(ds);
		saveDataSources();
		// saveDataSourcesToMongoDatabase();
		return result;
	}

	@Override
	public synchronized void updateDataSource(final String id, final DataSource ds) throws DataExtractionServiceException
	{
		super.updateDataSource(id, ds);
		saveDataSources();
		// saveDataSourcesToMongoDatabase();
	}

	@Override
	public synchronized void deleteDataSource(final String id)
	{
		try
		{
			super.deleteDataSource(id);
			saveDataSources();
			// saveDataSourcesToMongoDatabase();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}

	@Override
	public synchronized List<DataExtractionJob> getDataExtractionJobs()
	{

		List<DataExtractionJob> dataExtractionJobList = new ArrayList<DataExtractionJob>();

		List<DataExtractionJob> dataExtractionJobs = super.getDataExtractionJobs();

		for (DataExtractionJob dataExtractionJob : dataExtractionJobs)
		{

			dataExtractionJobList.add(getDataExtractionJob(dataExtractionJob.getId()));
		}

		return dataExtractionJobList;
	}

	@Override
	public synchronized DataExtractionJob getDataExtractionJob(String id)
	{

		DataExtractionJob dataExtractionJob = super.getDataExtractionJob(id);

		List<DataExtractionTaskResults> dataExtractionTaskResults = workQueueMonitoring.getDataExtractionTaskResults(dataExtractionJob);

		if( dataExtractionTaskResults.size() > 0 )
		{
			int chunksCount = dataExtractionTaskResults.size();

			int objectsExtracted = 0, failureMessageCount = 0, count = 1;

			String timeStarted = null, timeCompleted = null, lastFailureMessage = null;

			List<DataExtractionTaskResults> dataExtractionChunks = new ArrayList<DataExtractionTaskResults>();

			for (DataExtractionTaskResults dataExtractionChunk : dataExtractionTaskResults)
			{
				if( dataExtractionChunk.getJobId().equals(dataExtractionJob.getId()) )
				{
					if( count == 1 )

						timeStarted = dataExtractionChunk.getTimeStarted();

					if( count == chunksCount )

						timeCompleted = dataExtractionChunk.getTimeCompleted();

					dataExtractionChunks.add(dataExtractionChunk);

					objectsExtracted += dataExtractionChunk.getObjectsExtracted();

					lastFailureMessage = dataExtractionChunk.getLastFailureMessage();

					if( lastFailureMessage != null )
					{
						failureMessageCount++;
					}

					count++;
				}
			}
			dataExtractionJob.setObjectsExtracted(objectsExtracted);

			if( failureMessageCount > 0 )
			{
				dataExtractionJob.setState(StateEnum.FAILED);

			}
			else
			{
				if( dataExtractionJob.getTasksCount() == chunksCount )
				{
					if( objectsExtracted == dataExtractionJob.getObjectCount() )
					{

						dataExtractionJob.setState(StateEnum.SUCCEEDED);

					}
					else
					{

						dataExtractionJob.setState(StateEnum.RUNNING);

					}
				}
				else
				{
					dataExtractionJob.setState(StateEnum.RUNNING);
				}
			}
			dataExtractionJob.setTimeStarted(timeStarted);

			dataExtractionJob.setTimeCompleted(timeCompleted);

			dataExtractionJob.setDataExtractionTasks(dataExtractionChunks);
		}
		return dataExtractionJob;
	}

	private void deleteOutputMessagingQueue(final DataExtractionJob job) throws DataExtractionServiceException
	{
		if( job.getOutputMessagingQueue() == null )
		{
			return;
		}
		try ( MessagingConnection mc = messaging.connect())
		{
			synchronized (job)
			{
				MessagingQueue queue = mc.queue(job.getOutputMessagingQueue());
				queue.delete();
				job.setOutputMessagingQueue(null);
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownScheduleQueueConnection").message(e.getMessage()));
		}
	}

	void createDocument(MongoCollection<Document> mongoCollection) throws Exception
	{
		for (final DataSource ds : getDataSources())
		{
			Document document = new Document("id", ds.getId()).append("label", ds.getLabel()).append("type", ds.getType()).append("description", ds.getDescription()).append("connectionParams", encryptConnectionParams(ds.getConnectionParams()));
			mongoCollection.insertOne(document);
		}
	}
}
