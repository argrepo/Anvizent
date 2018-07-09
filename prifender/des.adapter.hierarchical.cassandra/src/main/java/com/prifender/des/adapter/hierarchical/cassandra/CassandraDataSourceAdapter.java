package com.prifender.des.adapter.hierarchical.cassandra;

import static com.prifender.des.CollectionName.fromSegments;
import static com.prifender.des.CollectionName.parse;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionContext;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
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
import com.prifender.des.model.ConnectionParamDef.TypeEnum;

/**
 * The CassandraDataSourceAdapter Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch simplified metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-05-09
 */
@Component
public class CassandraDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private final static String JOB_NAME = "local_project.prifender_cassandra_m3_v1_0_1.Prifender_Cassandra_M3_v1";

	private final static String DEPENDENCY_JAR = "prifender_cassandra_m3_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "Cassandra";
	private static final String TYPE_LABEL = "Apache Cassandra";

	// KeySpace

	public static final String PARAM_KEYSPACE_ID = "KeySpace";
	public static final String PARAM_KEYSPACE_LABEL = "KeySpace";
	public static final String PARAM_KEYSPACE_DESCRIPTION = "The name of a KeySpace";

	public static final ConnectionParamDef PARAM_KEYSPACE = new ConnectionParamDef().id(PARAM_KEYSPACE_ID).label(PARAM_KEYSPACE_LABEL).description(PARAM_KEYSPACE_DESCRIPTION).type(TypeEnum.STRING).required(false);

	// Read Timeout in Milli Seconds

	public static final String PARAM_READ_TIMEOUT_ID = "ReadTimeout";
	public static final String PARAM_READ_TIMEOUT_LABEL = "Read Timeout";
	public static final String PARAM_READ_TIMEOUT_DESCRIPTION = "The connect timeout in milliseconds for the underlying Netty channel";

	public static final ConnectionParamDef PARAM_READ_TIMEOUT = new ConnectionParamDef().id(PARAM_READ_TIMEOUT_ID).label(PARAM_READ_TIMEOUT_LABEL).description(PARAM_READ_TIMEOUT_DESCRIPTION).type(TypeEnum.INTEGER);

	// Connect Timeout in Milli Seconds

	public static final String PARAM_CONNECT_TIMEOUT_ID = "ConnectTimeout";
	public static final String PARAM_CONNECT_TIMEOUT_LABEL = "Connect Timeout";
	public static final String PARAM_CONNECT_TIMEOUT_DESCRIPTION = "The amount of time in milliseconds the driver waits for a given Cassandra node to answer a query before timing out";

	public static final ConnectionParamDef PARAM_CONNECT_TIMEOUT = new ConnectionParamDef().id(PARAM_CONNECT_TIMEOUT_ID).label(PARAM_CONNECT_TIMEOUT_LABEL).description(PARAM_CONNECT_TIMEOUT_DESCRIPTION).type(TypeEnum.INTEGER);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(clone(PARAM_PORT).required(false).defaultValue("9042")).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD)
			.addConnectionParamsItem(PARAM_KEYSPACE).addConnectionParamsItem(clone(PARAM_CONNECT_TIMEOUT).required(false).defaultValue("5000")).addConnectionParamsItem(clone(PARAM_READ_TIMEOUT).required(false).defaultValue("12000"));

	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	/**
	 * Returns the TestConnection Status for the Cassandra
	 */
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{

		Cluster cluster = null;
		Session session = null;
		List<KeyspaceMetadata> keyspaceMetadata = null;
		boolean keyspaceExist = false;
		try
		{
			cluster = getDataBaseConnection(ds);
			if( cluster != null )
			{
				session = getSession(cluster);
				keyspaceMetadata = session.getCluster().getMetadata().getKeyspaces();
				final String keyspace = getConnectionParam(ds, PARAM_KEYSPACE_ID);
				if( StringUtils.isNotBlank(keyspace) )
				{
					for (KeyspaceMetadata metadata : keyspaceMetadata)
					{
						if( metadata.getName().equals(keyspace) )
						{
							keyspaceExist = true;
							break;
						}
					}
					if( keyspaceExist )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Cassandra connection successfully established.");
					}
					else
					{
						throw new IllegalArgumentException("keyspace not found in '" + ds.getId() + "' datasource.");
					}
				}
				else
				{
					if( keyspaceMetadata.size() > 0 )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Cassandra connection successfully established.");
					}
					else
					{
						throw new IllegalArgumentException("keyspaces not found in '" + ds.getId() + "' datasource.");
					}
				}
			}
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnnection").message(e.getMessage()));
		}
		finally
		{

			destroyConnections(cluster, session);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Cassandra database");
	}

	/**
	 * Returns the Metadata for the Cassandra based on the connection params
	 */
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Cluster cluster = null;
		Session session = null;
		List<KeyspaceMetadata> keyspaceMetadata = null;
		boolean keyspaceExist = false;
		try
		{
			cluster = getDataBaseConnection(ds);
			if( cluster != null )
			{
				session = getSession(cluster);
				if( session != null )
				{
					keyspaceMetadata = session.getCluster().getMetadata().getKeyspaces();
					final String keyspace = getConnectionParam(ds, PARAM_KEYSPACE_ID);
					if( StringUtils.isNotBlank(keyspace) )
					{
						for (KeyspaceMetadata keyspacemetadata : keyspaceMetadata)
						{
							if( keyspacemetadata.getName().equals(keyspace) )
							{
								keyspaceExist = true;
								break;
							}
						}
						if( keyspaceExist )
						{
							metadata = metadataByConnection(cluster, keyspace);
						}
						else
						{
							throw new IllegalArgumentException("keyspace not found in '" + ds.getId() + "' datasource.");
						}
					}
					else
					{
						if( keyspaceMetadata.size() > 0 )
						{
							metadata = metadataByConnection(cluster, keyspace);
						}
						else
						{
							throw new IllegalArgumentException("keyspaces not found in '" + ds.getId() + "' datasource.");
						}
					}

				}
			}
			else
			{
				throw new DataExtractionServiceException(new Problem().code("unknownConnection").message("Could not connect to Cassandra database."));
			}
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnnection").message(e.getMessage()));
		}
		finally
		{
			destroyConnections(cluster, session);
		}
		return metadata;
	}

	/**
	 * Returns the numbers of rows available for a particular table
	 * 
	 * @param ds
	 * @param tableName
	 * @return int count
	 * @throws DataExtractionServiceException
	 */
	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{

		int count = 0;
		Cluster cluster = null;
		Session session = null;
		try
		{
			cluster = getDataBaseConnection(ds);

			if( cluster != null )
			{
				String tableName = spec.getCollection();
				String database = getConnectionParam(ds, PARAM_KEYSPACE_ID);
				String[] databaseTable = parse(tableName).segments();
				if( databaseTable.length == 2 )
				{
					database =  databaseTable[0];
					tableName =  databaseTable[1];
				}
				else
				{
					throw new IllegalArgumentException("Unknown collection '" + tableName + "'");
				}
				
				session = getSessionByKeySpace(cluster, database);
				if( session != null )
				{
					count = getTableRowCount(database, tableName, cluster, session);
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroyConnections(cluster, session);
		}
		return count;
	}

	private Metadata metadataByConnection(Cluster cluster, String keySpace) throws DataExtractionServiceException
	{

		Metadata metadata = new Metadata();
		List<String> keySpaceList = new ArrayList<String>();

		if( StringUtils.isNotBlank(keySpace) )
		{
			keySpaceList.add(keySpace);
		}
		else
		{
			keySpaceList = getKeySpaceList(cluster);
		}

		if( keySpaceList.size() == 0 )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownKeyspaces").message("No Keyspaces found."));
		}

		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (String schemaName : keySpaceList)
		{

			List<NamedType> tableList = getDatasourceRelatedTables(cluster, schemaName);

			for (NamedType namedType : tableList)
			{

				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);

				String tableName = getTableName(namedType.getName());
				List<NamedType> attributeListForColumns = getTableRelatedColumns(cluster, schemaName, tableName);

				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	private String getTableName(String schemaTableName)
	{
		String tableName = "";
		if( schemaTableName.contains(".") )
		{
			String[] schemaTableNameList = parse(schemaTableName).segments();
			tableName = schemaTableNameList[schemaTableNameList.length - 1];
		}
		return tableName;
	}

	/**
	 * For a given Cluster and the table it gets the columns information based
	 * on the dataType it calls the Specific methods and returns the column
	 * Related Data
	 * 
	 * @param cluster
	 * @param dataSource
	 * @param name
	 * @return List<NamedType>
	 */
	private List<NamedType> getTableRelatedColumns(Cluster cluster, String dataSource, String name)
	{
		List<NamedType> columnNamedTypeList = new ArrayList<NamedType>();

		List<ColumnMetadata> columnMetadataList = cluster.getMetadata().getKeyspace(dataSource).getTable(name).getColumns();

		if( columnMetadataList != null && columnMetadataList.size() > 0 )
		{
			for (ColumnMetadata colMetadata : columnMetadataList)
			{

				NamedType namedType = new NamedType();
				namedType.setName(colMetadata.getName());
				DataType colMetaType = colMetadata.getType();

				if( colMetaType.toString().contains("set") )
				{
					namedType.setType(new Type().kind(Type.KindEnum.LIST));
					columnNamedTypeList = getSubColumnDataType(colMetaType, namedType);
				}
				else if( colMetaType.toString().contains("map") )
				{
					namedType.setType(new Type().kind(Type.KindEnum.OBJECT));
					columnNamedTypeList = getSubColumnDataType(colMetaType, namedType);
				}
				else if( colMetaType.toString().contains("list") )
				{
					namedType.setType(new Type().kind(Type.KindEnum.LIST));
					columnNamedTypeList = getSubColumnDataType(colMetaType, namedType);
				}
				else
				{
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(colMetaType.toString().toUpperCase()));
					namedType.setType(typeForCoumn);
					columnNamedTypeList.add(namedType);
				}
			}
		}
		return columnNamedTypeList;
	}

	/**
	 * Calls the SubColumnRelatedValues Method based on the dataType and returns
	 * the columnRelated data
	 * 
	 * @param colMetaType
	 * @param namedType
	 * @return List<NamedType>
	 */
	private List<NamedType> getSubColumnDataType(DataType colMetaType, NamedType namedType)
	{
		List<NamedType> columnNamedTypeList = new ArrayList<NamedType>();

		Type entryType = new Type().kind(Type.KindEnum.OBJECT);
		namedType.getType().setEntryType(entryType);

		List<NamedType> attributeListForColumns = getSubColumnRelatedValues(colMetaType);
		entryType.setAttributes(attributeListForColumns);
		columnNamedTypeList.add(namedType);
		return columnNamedTypeList;
	}

	/**
	 * Based on the Type it Returns the column Related values
	 * 
	 * @param elementType
	 * @return List<NamedType>
	 */
	@SuppressWarnings("static-access")
	private List<NamedType> getSubColumnRelatedValues(DataType elementType)
	{

		List<NamedType> namedTypeList = new ArrayList<NamedType>();

		if( elementType.getTypeArguments().size() > 0 )
		{
			String type = elementType.getTypeArguments().get(0).getClass().getName().toString();
			if( type.contains("UserType") )
			{

				UserType userType = (UserType) elementType.list(elementType).getTypeArguments().iterator().next().getTypeArguments().iterator().next();
				if( userType != null )
				{
					Collection<String> subColDataList = userType.getFieldNames();
					for (String subColData : subColDataList)
					{
						NamedType namedType = new NamedType();
						namedType.setName(subColData);
						Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(userType.getFieldType(subColData).toString().toUpperCase()));
						namedType.setType(columnType);
						namedTypeList.add(namedType);
					}
				}
			}
			else
			{
				List<DataType> subArgDataTypeList = elementType.getTypeArguments();
				if( subArgDataTypeList != null && subArgDataTypeList.size() > 0 )
				{
					for (DataType subArgDataType : subArgDataTypeList)
					{
						NamedType namedType = new NamedType();
						String subType = subArgDataType.getName().toString().toUpperCase();
						Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(subType));
						namedType.setType(columnType);
						namedTypeList.add(namedType);
					}
				}
			}
		}
		else
		{
			NamedType namedType = new NamedType();
			String subType = elementType.getName().getClass().toString().toUpperCase();
			Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(subType));
			namedType.setType(columnType);
			namedTypeList.add(namedType);
		}

		return namedTypeList;
	}

	/**
	 * For a given Database retrives all the available Tables from the cluster
	 * and then adds all the table Related information in the list and returns
	 * the tableName list
	 * 
	 * @param cluster
	 * @param dataSource
	 * @return list<namedType> TableName List
	 */
	private List<NamedType> getDatasourceRelatedTables(Cluster cluster, String dataSource)
	{
		List<NamedType> tableMetadataList = new ArrayList<NamedType>();

		KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(dataSource);

		Collection<TableMetadata> keySpaceTableList = keyspaceMetadata.getTables();
		if( keySpaceTableList != null && keySpaceTableList.size() > 0 )
		{
			for (TableMetadata tableName : keySpaceTableList)
			{
				NamedType namedType = new NamedType();
				namedType.setName(fromSegments(dataSource, tableName.getName()).toString());
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableMetadataList.add(namedType);
			}
		}
		return tableMetadataList;
	}

	/**
	 * Returns the available KeySpace for the given cluster
	 * 
	 * @param cluster
	 * @return List<String> KeySpaces
	 */
	private List<String> getKeySpaceList(Cluster cluster)
	{
		List<String> keySpaceList = new ArrayList<String>();
		List<KeyspaceMetadata> keySpaceMetadataList = cluster.getMetadata().getKeyspaces();
		for (KeyspaceMetadata keySpaceMetadata : keySpaceMetadataList)
		{
			if( isWantedKeyspace(keySpaceMetadata.getName()) )
			{
				keySpaceList.add(keySpaceMetadata.getName());
			}
		}
		return keySpaceList;
	}

	public boolean isWantedKeyspace(String keyspace)
	{
		String[] defaultKeySpaces = {};
		List<String> defaultKeySpacesList = new ArrayList<>();
		defaultKeySpaces = new String[] { "system_traces", "system", "system_distributed", "system_schema", "system_auth" };
		boolean wantedKeyspace = true;
		for (int i = 0; i < defaultKeySpaces.length; i++)
		{
			defaultKeySpacesList.add(defaultKeySpaces[i]);
		}
		if( defaultKeySpacesList.contains(keyspace) )
		{
			wantedKeyspace = false;
		}
		return wantedKeyspace;
	}

	/**
	 * For a given cluster,the session executes the Total number of Records for
	 * a specific table
	 * 
	 * @param database
	 * @param tableName
	 * @param cluster
	 * @param session
	 * @return int
	 */
	private int getTableRowCount(String database, String tableName, Cluster cluster, Session session)
	{
		int count = 0;
		String query = "Select Count (1) from " + database +"."+tableName;
		ResultSet results = session.execute(query);
		while (results.iterator().hasNext())
		{
			Row row = results.one();
			if( row != null )
			{
				count = (int) row.getLong(0);
			}
		}
		return count;
	}

	/**
	 * Gets the Required connection Parameters inorder to establish connections
	 * for the cluster
	 * 
	 * @param ds
	 * @param cluster
	 * @return Cluster
	 * @throws DataExtractionServiceException
	 */
	private Cluster getDataBaseConnection(DataSource ds) throws DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}

		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String port = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final int connectTimedOut = Integer.parseInt(getConnectionParam(ds, PARAM_CONNECT_TIMEOUT_ID));
		final int readTimedOut = Integer.parseInt(getConnectionParam(ds, PARAM_READ_TIMEOUT_ID));

		return getDataBaseConnection(host, port, userName, password, connectTimedOut, readTimedOut);
	}

	/**
	 * Builds the Cluster based on the given authorization and the
	 * userCredentials
	 * 
	 * @param host
	 * @param userName
	 * @param password
	 * @param cluster
	 * @return Cluster
	 */
	private Cluster getDataBaseConnection(String host, String port, String userName, String password, int connectTimedOut, int readTimedOut)
	{
		Cluster cluster = Cluster.builder().addContactPoint(host).withPort(Integer.parseInt(port)).withCredentials(userName, password).build();
		cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(connectTimedOut).setReadTimeoutMillis(readTimedOut);
		return cluster;
	}

	/**
	 * Returns the Session for the given Cluster
	 * 
	 * @param cluster
	 * @param database
	 * @return
	 */
	private Session getSession(Cluster cluster)
	{
		return cluster.connect();
	}

	private Session getSessionByKeySpace(Cluster cluster, String keySpace)
	{
		return cluster.connect(keySpace);
	}

	/**
	 * Destroys the connection for the given Cluster and the Session if exists
	 * 
	 * @param cluster
	 * @param session
	 */
	private void destroyConnections(Cluster cluster, Session session)
	{
		if( cluster != null )
		{
			cluster.close();
		}
		if( session != null )
		{
			session.close();
		}
	}

	/**
	 * Executes the jobs based on the DataSource and the DataExtractionSpec
	 */
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName, TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new CassandraDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class CassandraDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public CassandraDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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
		 * Based on the Scope and the SampleSize of the Specification it
		 * executes the DataExtraction Jobs
		 * 
		 * @param ds
		 * @param spec
		 * @param job
		 * @param rowCount
		 * @param adapterHome
		 * @param containersCount
		 * @return List<DataExtractionTask>
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
		 * Gets the Required Connection params and executes the Data Extraction
		 * Jobs
		 * 
		 * @param ds
		 * @param spec
		 * @param job
		 * @param adapterHome
		 * @param offset
		 * @param limit
		 * @return DataExtractionTask
		 * @throws DataExtractionServiceException
		 */
		private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec,

				DataExtractionJob job, String adapterHome, int offset, int limit) throws DataExtractionServiceException
		{

			DataExtractionTask dataExtractionTask = new DataExtractionTask();

			try
			{

				String dataSourceTableName = spec.getCollection();

				final String dataSourceUser = getConnectionParam(ds, PARAM_USER_ID);

				final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID);

				final String dataSourcePassword = getConnectionParam(ds, PARAM_PASSWORD_ID);

				final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);

				String databaseName = getConnectionParam(ds, PARAM_KEYSPACE_ID);
				
				final int readTimedOut = Integer.parseInt(getConnectionParam(ds, PARAM_READ_TIMEOUT_ID));

				final int connectTimedOut = Integer.parseInt(getConnectionParam(ds, PARAM_CONNECT_TIMEOUT_ID));
				
				String[] databaseTable = StringUtils.split(dataSourceTableName, ".");

				if( databaseTable.length == 2 )
				{

					databaseName = databaseTable[0].replaceAll(" ", "\\ ");;

					dataSourceTableName = databaseTable[1].replaceAll(" ", "\\ ");;

				}

				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser, dataSourcePassword, dataSourceTableName,

						getFormulateDataSourceColumnNames(ds, spec, "[*].", "@#@").replaceAll(" ", "\\ "), dataSourceHost, dataSourcePort, databaseName, job.getOutputMessagingQueue(), String.valueOf(readTimedOut) , String.valueOf(connectTimedOut) ,String.valueOf(offset),

						String.valueOf(limit), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE), String.valueOf(limit));
			 

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
		 * Returns the Context Params map for the job extraction
		 * 
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

				String dataSourcePassword, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost, String dataSourcePort,

				String dataSourceName, String jobId,String readTimedOut,String connectTimedOut , String offset, String limit, String dataSourceScope, String dataSourceSampleSize) throws IOException
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

			ilParamsVals.put("READ_TIMEOUT", readTimedOut);
			
			ilParamsVals.put("CONNECT_TIMEOUT", connectTimedOut);
			
			ilParamsVals.put("JOB_ID", jobId);

			ilParamsVals.put("OFFSET", offset);

			ilParamsVals.put("LIMIT", limit);

			ilParamsVals.put("SCOPE", dataSourceScope);

			ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

			return ilParamsVals;

		}

	}

}
