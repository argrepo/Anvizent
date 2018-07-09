package com.prifender.des.adapter.nosql.hbase;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionContext;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
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
 * The HBaseDataSourceAdapter Component implements
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
public final class HBaseDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final String JOB_NAME = "local_project.prifender_hbase_m2_v1_0_1.Prifender_Hbase_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_hbase_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "HBase";
	private static final String TYPE_LABEL = "Apache HBase";

	// Zookeeper Quorum

	public static final String PARAM_ZOOKEEPER_QUORUM_ID = "ZookeeperQuorum";
	public static final String PARAM_ZOOKEEPER_QUORUM_LABEL = "Zookeeper Quorum";
	public static final String PARAM_ZOOKEEPER_QUORUM_DESCRIPTION = "The name of the zookeeper quorum";

	public static final ConnectionParamDef PARAM_ZOOKEEPER_QUORUM = new ConnectionParamDef().id(PARAM_ZOOKEEPER_QUORUM_ID).label(PARAM_ZOOKEEPER_QUORUM_LABEL).description(PARAM_ZOOKEEPER_QUORUM_DESCRIPTION).type(TypeEnum.STRING);

	// Zookeeper Property Client Port

	public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID = "ZookeeperPropertyClientPort";
	public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_LABEL = "Zookeeper property client port";
	public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_DESCRIPTION = "The port at which the clients will connect";

	public static final ConnectionParamDef PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT = new ConnectionParamDef().id(PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID).label(PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_LABEL).description(PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_DESCRIPTION).type(TypeEnum.STRING);

	// hbase client scanner caching

	public static final String PARAM_CLIENT_SCANNER_CACHING_ID = "ClientScannerCaching";
	public static final String PARAM_CLIENT_SCANNER_CACHING_LABEL = "Client scanner caching";
	public static final String PARAM_CLIENT_SCANNER_CACHING_DESCRIPTION = "Number of rows that will be fetched when calling next on a scanner if it is not served from (local, client) memory";

	public static final ConnectionParamDef PARAM_CLIENT_SCANNER_CACHING = new ConnectionParamDef().id(PARAM_CLIENT_SCANNER_CACHING_ID).label(PARAM_CLIENT_SCANNER_CACHING_LABEL).description(PARAM_CLIENT_SCANNER_CACHING_DESCRIPTION).type(TypeEnum.INTEGER).required(false);

	// hbase client RPC Timeout

	public static final String PARAM_CLIENT_RPC_TIMEDOUT_ID = "RPCTimeout";
	public static final String PARAM_CLIENT_RPC_TIMEDOUT_LABEL = "RPC Timeout";
	public static final String PARAM_CLIENT_RPC_TIMEDOUT_DESCRIPTION = "This is for the RPC layer to define how long HBase client applications take for a remote call to time out";

	public static final ConnectionParamDef PARAM_CLIENT_RPC_TIMEDOUT = new ConnectionParamDef().id(PARAM_CLIENT_RPC_TIMEDOUT_ID).label(PARAM_CLIENT_RPC_TIMEDOUT_LABEL).description(PARAM_CLIENT_RPC_TIMEDOUT_DESCRIPTION).type(TypeEnum.INTEGER).required(false);

	public static final String PARAM_TABLENAME_ID = "TableName";
	public static final String PARAM_TABLENAME_LABEL = "Table Name";
	public static final String PARAM_TABLENAME_DESCRIPTION = "The name of the Table";

	public static final ConnectionParamDef PARAM_TABLENAME = new ConnectionParamDef().id(PARAM_TABLENAME_ID).label(PARAM_TABLENAME_LABEL).description(PARAM_TABLENAME_DESCRIPTION).type(TypeEnum.STRING).required(false);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_ZOOKEEPER_QUORUM).addConnectionParamsItem(PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT)
			.addConnectionParamsItem(clone(PARAM_CLIENT_SCANNER_CACHING).defaultValue("1000")).addConnectionParamsItem(clone(PARAM_CLIENT_RPC_TIMEDOUT).defaultValue("600000")).addConnectionParamsItem(PARAM_TABLENAME);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		Connection connection = null;
		try
		{
			connection = getDataBaseConnection(ds);
			if( connection != null )
			{
				String tableName = getConnectionParam(ds, PARAM_TABLENAME_ID);
				List<String> availableTableList = getAvailableTableList(connection.getAdmin(), tableName);

				if( availableTableList != null )
				{
					if( availableTableList.size() > 0 )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Hbase connection successfully established.");
					}
					else
					{
						throw new IllegalArgumentException("Table  '" + tableName + "' not found in datasource '" + ds.getId() + "'");
					}
				}
				else
				{
					throw new IllegalArgumentException("No table found in '" + ds.getId() + "' dataSource.");
				}
			}
		}
		catch ( IOException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		catch ( IllegalArgumentException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(connection);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Hbase database");
	}

	public void destroy(Connection conn)
	{
		if( conn != null )
		{
			try
			{
				conn.getAdmin().close();
				conn.getConfiguration().clear();
				conn.close();
			}
			catch ( Exception e )
			{
				e.printStackTrace();
			}
		}
	}

	private Connection getDataBaseConnection(DataSource ds) throws DataExtractionServiceException, IOException
	{
		Connection con = null;
		try
		{
			if( ds == null )
			{
				throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
			}
			final String hbaseZookeeperQuorum = getConnectionParam(ds, PARAM_ZOOKEEPER_QUORUM_ID);
			final String hbaseZookeeperPropertyClientPort = getConnectionParam(ds, PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID);
			final String hbaseUser = getConnectionParam(ds, PARAM_USER_ID);
			final int scannerCaching = Integer.parseInt(getConnectionParam(ds, PARAM_CLIENT_SCANNER_CACHING_ID));
			final int rPCTimeout = Integer.parseInt(getConnectionParam(ds, PARAM_CLIENT_RPC_TIMEDOUT_ID));

			File file = new File(".");
			System.getProperties().put("hadoop.home.dir", file.getAbsolutePath());
			new File("./bin").mkdirs();
			new File("./bin/winutils.exe").createNewFile();

			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
			config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperPropertyClientPort);
			config.setLong("hbase.client.scanner.caching", scannerCaching);
			config.setLong("hbase.rpc.timeout", rPCTimeout);

			UserGroupInformation.setConfiguration(config);

			HBaseAdmin.checkHBaseAvailable(config);

			if( StringUtils.isNotBlank(hbaseUser) )
			{
				System.setProperty("HADOOP_USER_NAME", hbaseUser);
				con = ConnectionFactory.createConnection(config);
			}
			else
			{
				con = ConnectionFactory.createConnection(config);
			}

		}
		catch ( ConnectException | UnknownHostException une )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(une.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return con;
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{
		Connection connection = null;
		Metadata metadata = null;
		try
		{
			connection = getDataBaseConnection(ds);
			if( connection != null )
			{
				String tableName = getConnectionParam(ds, PARAM_TABLENAME_ID);
				metadata = metadataByConnection(connection.getAdmin(), connection.getConfiguration(), tableName, ds.getId());
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownCollection").message(e.getMessage()));
		}
		finally
		{
			destroy(connection);
		}
		return metadata;
	}

	@SuppressWarnings("deprecation")
	@Override
	public int getCountRows(DataSource ds,DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		Connection connection = null;
		AggregationClient aggregationClient = null;
		try
		{
			String tableName = spec.getCollection();
			connection = getDataBaseConnection(ds);
			if( connection != null )
			{
				aggregationClient = new AggregationClient(connection.getConfiguration());
				countRows = (int) aggregationClient.rowCount(new HTable(connection.getConfiguration(), tableName), new LongColumnInterpreter(), new Scan());
			}
		}
		catch ( Throwable e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			if( aggregationClient != null )
			{
				try
				{
					aggregationClient.close();
				}
				catch ( IOException e )
				{
					e.printStackTrace();
				}
			}
			destroy(connection);
		}
		return countRows;
	}

	private List<String> getAvailableTableList(Admin hBaseAdmin, String tableName) throws DataExtractionServiceException
	{
		List<String> tableNameList = null;
		try
		{
			tableNameList = getTableNameList(hBaseAdmin);

			if( StringUtils.isNotBlank(tableName) )
			{
				if( tableNameList != null && tableNameList.size() > 0 )
				{
					for (String tblNme : tableNameList)
					{
						tableNameList = new ArrayList<>();
						if( tblNme.equals(tableName) )
						{
							tableNameList.add(tableName);
							break;
						}
					}
				}
				return tableNameList;
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadta").message(e.getMessage()));
		}
		return tableNameList;

	}

	private Metadata metadataByConnection(Admin hBaseAdmin, Configuration config, String tableName, String dataSourceId) throws DataExtractionServiceException
	{
		Metadata metadata = new Metadata();
		try
		{
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			List<String> availableTableNameList = getAvailableTableList(hBaseAdmin, tableName);

			if( availableTableNameList != null )
			{
				List<NamedType> tableList = getDatasourceRelatedTables(availableTableNameList);
				for (NamedType namedType : tableList)
				{
					Type entryType = entryType(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), config);

					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					metadata.setObjects(namedTypeObjectsList);
				}
			}
			else
			{
				throw new IllegalArgumentException("No table found in datasource '" + dataSourceId + "'");
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadta").message(e.getMessage()));
		}
		return metadata;
	}

	private Type entryType(KindEnum kindEnum)
	{
		return new Type().kind(kindEnum);
	}

	@SuppressWarnings("deprecation")
	private List<NamedType> getTableRelatedColumns(String tableName, Configuration config) throws DataExtractionServiceException, IOException
	{
		List<NamedType> attributeList = new ArrayList<NamedType>();
		HTable hTable = null;
		Set<String> columns = new HashSet<String>();
		try
		{
			hTable = new HTable(config, tableName);

			ResultScanner results = hTable.getScanner(new Scan().setFilter(new PageFilter(10000)));
			for (Result result : results)
			{
				for (KeyValue keyValue : result.list())
				{
					columns.add(Bytes.toString(keyValue.getFamily()) + ":" + Bytes.toString(keyValue.getQualifier()));
				}
			}
			for (String column : columns)
			{
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(column);
				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(column.getClass().getSimpleName()));
				attributeForColumn.setType(typeForCoumn);
				attributeList.add(attributeForColumn);
			}

		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadata").message(e.getMessage()));
		}
		finally
		{
			if( hTable != null )
			{
				hTable.close();
			}
		}
		return attributeList;

	}

	private List<String> getTableNameList(Admin hBaseAdmin) throws DataExtractionServiceException
	{
		List<String> tableNameList = new ArrayList<String>();
		try
		{
			HTableDescriptor[] tableDescriptorList = hBaseAdmin.listTables();
			for (HTableDescriptor tDescriptor : tableDescriptorList)
			{
				tableNameList.add(tDescriptor.getNameAsString());
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadata").message(e.getMessage()));
		}
		return tableNameList;
	}

	private List<NamedType> getDatasourceRelatedTables(List<String> tableNameList) throws DataExtractionServiceException
	{
		List<NamedType> tableList = new ArrayList<>();
		try
		{
			for (String tableName : tableNameList)
			{
				NamedType namedType = new NamedType();
				namedType.setName(tableName);
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadata").message(e.getMessage()));
		}
		return tableList;
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

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName,TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new HBaseDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class HBaseDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public HBaseDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

				final String dataSourceHost = getConnectionParam(ds, PARAM_ZOOKEEPER_QUORUM_ID);

				final String dataSourcePort = getConnectionParam(ds, PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID);

				String tableName = spec.getCollection().replaceAll(" ", "\\ ");

				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME,

						tableName, getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), dataSourceHost, dataSourcePort, job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

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
		 * @param dataSourceTableName
		 * @param dataSourceColumnNames
		 * @param dataSourceHost
		 * @param dataSourcePort
		 * @param jobId
		 * @param offset
		 * @param limit
		 * @param dataSourceScope
		 * @param dataSourceSampleSize
		 * @return
		 * @throws IOException
		 */
		public Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,

				String dataSourcePort, String jobId, String offset, String limit,

				String dataSourceScope, String dataSourceSampleSize) throws IOException
		{

			Map<String, String> ilParamsVals = new LinkedHashMap<>();

			ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

			ilParamsVals.put("FILE_PATH", jobFilesPath);

			ilParamsVals.put("JOB_NAME", jobName);

			ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);

			ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);

			ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);

			ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);

			ilParamsVals.put("JOB_ID", jobId);

			ilParamsVals.put("OFFSET", offset);

			ilParamsVals.put("LIMIT", limit);

			ilParamsVals.put("SCOPE", dataSourceScope);

			ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

			return ilParamsVals;

		}
	}
}
