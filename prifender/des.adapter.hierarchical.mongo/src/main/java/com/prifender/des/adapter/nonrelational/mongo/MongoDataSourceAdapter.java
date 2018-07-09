package com.prifender.des.adapter.nonrelational.mongo;

import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.CollectionName.fromSegments;
import static com.prifender.des.CollectionName.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.stereotype.Component;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionContext;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;
import com.prifender.des.model.Type.DataTypeEnum;
import com.prifender.des.model.Type.KindEnum;

/**
 * The MongoDataSourceAdapter Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch hierarchical metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-04-06
 */
@Component
public final class MongoDataSourceAdapter extends DataSourceAdapter
{

	private final static String JOB_NAME = "local_project.prifender_mongodb_m2_v1_0_1.Prifender_MONGODB_M2_v1";

	private final static String DEPENDENCY_JAR = "prifender_mongodb_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "Mongo";
	public static final String TYPE_LABEL = "MongoDB";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(clone(PARAM_PORT).required(false).defaultValue("27017")).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD)
			.addConnectionParamsItem(PARAM_DATABASE);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		MongoClient mongoClient = null;
		List<String> dataSourceList = null;
		MongoCursor<Document> documents = null;
		try
		{
			mongoClient = getDataBaseConnection(ds);
			if( mongoClient != null )
			{
				try
				{
					final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);

					if( StringUtils.isNotBlank(databaseName) )
					{
						dataSourceList = new ArrayList<String>();
						dataSourceList.add(databaseName);
					}
					else
					{
						dataSourceList = getAllDatasourcesFromDatabase(mongoClient);
					}

					if( dataSourceList == null )
					{
						throw new IllegalArgumentException("Database '" + databaseName + "' not found.");
					}

					MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
					documents = mongoDatabase.listCollections().iterator();
					if( documents != null )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Mongo connection successfully established.");
					}
					else
					{
						throw new IllegalArgumentException("No Documents found in database '" + databaseName + "'.");
					}

				}
				catch ( MongoSecurityException e )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
				}
				catch ( MongoTimeoutException timeout )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(timeout.getMessage());
				}
				catch ( MongoException e )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
				}
				catch ( IllegalArgumentException iae )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(iae.getMessage());
				}
			}
		}
		finally
		{
			destroy(mongoClient, documents);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Mongo database");
	}

	private void destroy(MongoClient mongoClient, MongoCursor<Document> documents)
	{
		try
		{

			if( documents != null )
			{
				documents.close();
			}
			if( mongoClient != null )
			{
				mongoClient.close();
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}

	private void destroy(MongoClient mongoClient)
	{
		try
		{
			if( mongoClient != null )
			{
				mongoClient.close();
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}

	private void destroy(MongoCursor<Document> documents)
	{
		try
		{
			if( documents != null )
			{
				documents.close();
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{
		MongoClient mongoClient = null;
		Metadata metadata = null;
		try
		{
			final String dataSourceName = getConnectionParam(ds, PARAM_DATABASE_ID);
			mongoClient = getDataBaseConnection(ds);
			if( mongoClient != null )
			{
				metadata = metadataByConnection(mongoClient, dataSourceName);
			}
		}
		catch ( MongoSecurityException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( MongoTimeoutException timeout )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(timeout.getMessage()));
		}
		catch ( MongoException me )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(me.getMessage()));
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		finally
		{
			destroy(mongoClient);
		}
		return metadata;
	}

	@Override
	public int getCountRows(DataSource ds,DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		MongoClient mongoClient = null;
		try
		{
			String tableName = spec.getCollection();
			mongoClient = getDataBaseConnection(ds);
			if( mongoClient != null )
			{
				String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
				String[] dataSourceDocumet = parse(tableName).segments();
				if( dataSourceDocumet.length == 2 )
				{
					databaseName = dataSourceDocumet[0];
					tableName = dataSourceDocumet[1];
				}
				countRows = getCountRows(tableName, mongoClient, databaseName);
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(mongoClient);
		}
		return countRows;

	}

	private MongoClient getDataBaseConnection(DataSource ds) throws DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);

		return getDataBaseConnection(hostName, Integer.parseInt(portNumber), databaseName, userName, password);
	}

	@SuppressWarnings("deprecation")
	private MongoClient getDataBaseConnection(String hostName, int portNumber, String databaseName, String userName, String password)
	{
		MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
		optionsBuilder.readPreference(ReadPreference.primary());
		MongoClientOptions clientOptions = optionsBuilder.build();
		ServerAddress serverAddress = new ServerAddress(hostName, portNumber);
		MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(userName, databaseName, password.toCharArray());
		return new MongoClient(serverAddress, Arrays.asList(mongoCredential), clientOptions);
	}

	private Metadata metadataByConnection(MongoClient mongoClient, String databaseName) throws DataExtractionServiceException
	{

		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		MongoCursor<Document> documents = null;
		try
		{

			if( StringUtils.isNotBlank(databaseName) )
			{
				dataSourceList = new ArrayList<String>();
				dataSourceList.add(databaseName);
			}
			else
			{
				dataSourceList = getAllDatasourcesFromDatabase(mongoClient);
			}

			if( dataSourceList == null )
			{
				throw new IllegalArgumentException("Database '" + databaseName + "' not found.");
			}

			MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
			documents = mongoDatabase.listCollections().iterator();
			if( documents == null )
			{
				throw new IllegalArgumentException("No Documents found in database '" + databaseName + "'.");
			}

			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String dataSource : dataSourceList)
			{
				List<NamedType> tableList = getDatasourceRelatedTables(dataSource, mongoClient);
				for (NamedType namedType : tableList)
				{
					if( !StringUtils.equals(namedType.getName().toString(), fromSegments(dataSource, fromSegments("system").toString()).toString()) && !StringUtils.equals(namedType.getName().toString(), fromSegments(dataSource, fromSegments("system", "users").toString()).toString())
							&& !StringUtils.equals(namedType.getName().toString(), fromSegments(dataSource, fromSegments("system", "version").toString()).toString()) )
					{
						Type entryType = entryType(Type.KindEnum.OBJECT);
						namedType.getType().setEntryType(entryType);
						List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), mongoClient);
						entryType.setAttributes(attributeListForColumns);
						namedTypeObjectsList.add(namedType);
						metadata.setObjects(namedTypeObjectsList);
					}
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			if( documents != null )
			{
				destroy(documents);
			}
		}
		return metadata;
	}

	private List<String> getAllDatasourcesFromDatabase(MongoClient mongoClient) throws DataExtractionServiceException
	{
		List<String> schemaList = null;
		MongoCursor<String> dbsCursor = null;
		try
		{
			dbsCursor = mongoClient.listDatabaseNames().iterator();
			schemaList = new ArrayList<String>();
			while (dbsCursor.hasNext())
			{
				schemaList.add(dbsCursor.next());
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			if( dbsCursor != null )
			{
				dbsCursor.close();
			}
		}
		return schemaList;
	}

	private List<NamedType> getDatasourceRelatedTables(String schemaName, MongoClient mongoClient) throws DataExtractionServiceException
	{
		MongoDatabase mongoDatabase = null;
		MongoCursor<Document> document = null;
		List<NamedType> tableList = new ArrayList<>();
		try
		{
			mongoDatabase = mongoClient.getDatabase(schemaName);
			document = mongoDatabase.listCollections().iterator();
			while (document.hasNext())
			{
				NamedType namedType = new NamedType();
				namedType.setName(fromSegments(mongoDatabase.getName(), document.next().get("name").toString()).toString());
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(document);
		}
		return tableList;
	}

	@SuppressWarnings("rawtypes")
	private List<NamedType> getTableRelatedColumns(String tableName, MongoClient mongoClient) throws DataExtractionServiceException
	{
		MongoDatabase mongoDatabase = null;
		List<NamedType> namedTypeList = null;
		MongoCollection<Document> collection = null;
		try
		{
			mongoDatabase = mongoClient.getDatabase(parse(tableName).segment(0));
			collection = mongoDatabase.getCollection(parse(tableName).segment(1));
			MapReduceIterable iterable = getDocumentFieldNames(collection);
			List<String> fieldsSet = new ArrayList<String>();
			for (Object obj : iterable)
			{
				fieldsSet.add(((Document) obj).get("_id").toString());
			}
			namedTypeList = getFieldsNamesAndDataTypeList(fieldsSet, collection);
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return namedTypeList;
	}

	@SuppressWarnings("rawtypes")
	private MapReduceIterable getDocumentFieldNames(MongoCollection<Document> collection)
	{
		String map = "function() {   \n" + "for (var key in this)\n" + "{ emit(key, null); }; } ";
		String reduce = " function(key, stuff) { return null; }";
		return collection.mapReduce(map, reduce);
	}

	private List<NamedType> getFieldsNamesAndDataTypeList(List<String> fieldsSet, MongoCollection<Document> collection)
	{
		List<NamedType> namedTypeList = new ArrayList<>();
		for (String field : fieldsSet)
		{
			NamedType namedType = new NamedType();
			DBObject query = new BasicDBObject();
			query.put(field, new BasicDBObject("$ne", ""));
			FindIterable<Document> cursor = collection.find((Bson) query).projection(Projections.include(field)).limit(1);
			List<Document> documentsList = cursor.into(new LinkedList<>());
			if( documentsList.size() == 0 ) continue;
			if( documentsList.get(0).get(field) != null )
			{
				namedType = getDocFieldsAndValues(documentsList, field);
				namedTypeList.add(namedType);
			}
		}
		return namedTypeList;
	}

	@SuppressWarnings("unchecked")
	private NamedType getDocFieldsAndValues(List<Document> documentsList, String field)
	{

		NamedType namedType = new NamedType();
		String type = documentsList.get(0).get(field).getClass().getSimpleName();
		namedType.setName(field);

		if( type.equals("ArrayList") || type.equals("Object") )
		{

			for (Document document : documentsList)
			{
				List<Object> docFieldList = (ArrayList<Object>) document.get(field);

				namedType.setType(entryType(Type.KindEnum.LIST));
				Type entryType = entryType(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);

				String docType = docFieldList.get(0).getClass().getSimpleName().toString();

				if( StringUtils.equals("Document", docType) )
				{
					List<NamedType> attributeListForColumns = getSubDocFieldsAndValues((Document) docFieldList.get(0));
					entryType.setAttributes(attributeListForColumns);
				}
				else
				{
					namedType.setName(field);
					namedType.setType(entryType(Type.KindEnum.LIST));
					namedType.getType().setEntryType(new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(docType)));
				}
			}
		}
		else
		{
			namedType.setType(getTypeForColumn(type));
		}
		return namedType;
	}

	private List<NamedType> getSubDocFieldsAndValues(Document subDocument)
	{
		List<NamedType> attributeListForColumns = new ArrayList<>();
		for (Map.Entry<String, Object> set : subDocument.entrySet())
		{
			NamedType namedType = new NamedType();
			namedType.setName(set.getKey());
			if( set.getValue() != null )
			{
				Object object = subDocument.get(set.getKey());
				namedType.setType(getTypeForColumn(object.getClass().getSimpleName()));
			}
			attributeListForColumns.add(namedType);
		}
		return attributeListForColumns;
	}

	private Type entryType(KindEnum kindEnum)
	{
		return new Type().kind(kindEnum);
	}

	private Type getTypeForColumn(String type)
	{
		return entryType(Type.KindEnum.VALUE).dataType(getDataType(type));
	}

	private DataTypeEnum getDataType(String dataType)
	{
		DataTypeEnum dataTypeEnum = null;
		if( dataType.equals("INT") || dataType.equals("number") || dataType.equals("Number") || dataType.equals("Integer") || dataType.equals("INTEGER") )
		{
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		}
		else if( dataType.equals("Decimal") )
		{
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		}
		else if( dataType.equals("Float") || dataType.equals("Double") || dataType.equals("Numeric") || dataType.equals("Long") || dataType.equals("Real") )
		{
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		}
		else if( dataType.equals("String") )
		{
			dataTypeEnum = Type.DataTypeEnum.STRING;
		}
		else if( dataType.equals("Boolean") )
		{
			dataTypeEnum = Type.DataTypeEnum.BOOLEAN;
		}
		return dataTypeEnum;
	}

	@SuppressWarnings("rawtypes")
	private int getCountRows(String collectionName, MongoClient mongoClient, String dataSourceName)
	{
		MongoDatabase mongoDatabase = mongoClient.getDatabase(dataSourceName);
		MongoCollection collection = mongoDatabase.getCollection(collectionName);
		Long collectionCount = collection.count();
		return collectionCount.intValue();
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName,TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new MongoDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class MongoDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public MongoDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
		{
			super(context);
			this.adapterHome = adapterHome;
			this.containersCount = containersCount;
		}

		@Override
		protected List<DataExtractionTask> runDataExtractionJob() throws Exception
		{
			final DataSource ds = this.context.ds;
			final DataExtractionSpec spec = this.context.spec;
			final DataExtractionJob job = this.context.job;

			final int objectCount;

			synchronized (job)
			{
				objectCount = job.getObjectCount();
			}
			return getDataExtractionTasks(ds, spec, job, objectCount, adapterHome, containersCount);
		}

		/**
		 * @param ds
		 * @param spec
		 * @param job
		 * @param rowCount
		 * @param adapterHome
		 * @param containersCount
		 * @return
		 * @throws DataExtractionServiceException
		 */
		private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec,

				DataExtractionJob job, int objectCount, String adapterHome, int containersCount) throws DataExtractionServiceException
		{

			List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();

			try
			{
				if( objectCount <= MIN_THRESHOULD_ROWS )
				{
					dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, 1, objectCount));
				}
				else
				{
					int taskSampleSize = generateTaskSampleSize(objectCount, containersCount);

					if( taskSampleSize <= MIN_THRESHOULD_ROWS )
					{
						taskSampleSize = MIN_THRESHOULD_ROWS;
					}
					if( taskSampleSize > MAX_THRESHOULD_ROWS )
					{
						taskSampleSize = MAX_THRESHOULD_ROWS;
					}

					int noOfTasks = objectCount / taskSampleSize;

					int remainingSampleSize = objectCount % taskSampleSize;

					for (int i = 0; i < noOfTasks; i++)
					{

						int offset = taskSampleSize * i + 1;

						dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, offset, taskSampleSize));

					}

					if( remainingSampleSize > 0 )
					{
						int offset = noOfTasks * taskSampleSize + 1;

						dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, offset, remainingSampleSize));

					}
				}

			}
			catch ( Exception e )
			{
				throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
			}
			return dataExtractionJobTasks;
		}

		/**
		 * @param ds
		 * @param spec
		 * @param job
		 * @param adapterHome
		 * @param offset
		 * @param limit
		 * @return
		 * @throws DataExtractionServiceException
		 */
		private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec,

				DataExtractionJob job, String adapterHome, int offset, int limit) throws DataExtractionServiceException
		{

			DataExtractionTask dataExtractionTask = new DataExtractionTask();

			try
			{

				final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID);

				final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);

				final String dataSourceUser = getConnectionParam(ds, PARAM_USER_ID);

				final String dataSourcePassword = getConnectionParam(ds, PARAM_PASSWORD_ID);

				String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);

				String tableName = spec.getCollection();

				String[] databaseDocumet = parse(tableName).segments();

				if( databaseDocumet.length == 2 )
				{
					databaseName = databaseDocumet[0].replaceAll(" ", "\\ ");
					tableName = databaseDocumet[1].replaceAll(" ", "\\ ");
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownCollection").message("Unknown collection '" + tableName + "'"));
				}
				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,

						dataSourcePassword, tableName, getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), dataSourceHost, dataSourcePort,

						databaseName, job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

						String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE), String.valueOf(limit));

				dataExtractionTask.taskId("DES-Task-" + getUUID())

						.jobId(job.getId())

						.typeId(TYPE_ID + "__" + JOB_NAME + "__" + DEPENDENCY_JAR)

						.contextParameters(contextParams)

						.numberOfFailedAttempts(0);
			}
			catch ( Exception e )
			{
				throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
			}
			return dataExtractionTask;
		}

		/**
		 * @param jobFilesPath
		 * @param jobName
		 * @param dataSourceUser
		 * @param dataSourcePassword
		 * @param dataSourceTableName
		 * @param dataSourceColumnNames
		 * @param dataSourceHost
		 * @param dataSourcePort
		 * @param dataSourceName
		 * @param jobId
		 * @param offset
		 * @param limit
		 * @param dataSourceScope
		 * @param dataSourceSampleSize
		 * @return
		 * @throws IOException
		 */
		public Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceUser,

				String dataSourcePassword, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,

				String dataSourcePort, String dataSourceName, String jobId, String offset, String limit,

				String dataSourceScope, String dataSourceSampleSize) throws IOException
		{

			Map<String, String> ilParamsVals = new LinkedHashMap<>();

			ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

			ilParamsVals.put("FILE_PATH", jobFilesPath);

			ilParamsVals.put("JOB_NAME", jobName);

			ilParamsVals.put("DATASOURCE_USER", dataSourceUser);

			ilParamsVals.put("DATASOURCE_PASS", dataSourcePassword);

			ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);

			ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);

			ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);

			ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);

			ilParamsVals.put("DATASOURCE_NAME", dataSourceName);

			ilParamsVals.put("JOB_ID", jobId);

			ilParamsVals.put("OFFSET", offset);

			ilParamsVals.put("LIMIT", limit);

			ilParamsVals.put("SCOPE", dataSourceScope);

			ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

			return ilParamsVals;

		}
	}
}
