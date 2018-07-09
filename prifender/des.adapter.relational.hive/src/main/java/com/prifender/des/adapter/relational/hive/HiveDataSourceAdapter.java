package com.prifender.des.adapter.relational.hive;

import static com.prifender.des.util.DatabaseUtil.closeSqlObject;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getObjectCount;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import static com.prifender.des.CollectionName.parse;
import static com.prifender.des.util.DatabaseUtil.getNameByPattern;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionContext;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.Constraint;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;

/**
 * The HiveDataSourceAdapter Component implements
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
public final class HiveDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final String JOB_NAME = "local_project.prifender_hive_m2_v1_0_1.Prifender_Hive_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_hive_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "Hive";

	private static final String TYPE_LABEL = "Apache Hive";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(PARAM_PORT).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD).addConnectionParamsItem(PARAM_SCHEMA);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException
	{
		Connection connection = null;
		List<String> schemaList = null;
		boolean checkSchemaExist = false;
		try
		{
			connection = getDataBaseConnection(ds);
			schemaList = getScheamsFromDatabase(connection);
			if( schemaList != null && schemaList.size() == 0 )
			{
				throw new IllegalArgumentException("No schema's found in '" + ds.getId() + "' dataSource.");
			}
			final String schema = getConnectionParam(ds, PARAM_SCHEMA_ID);
			checkSchemaExist = checkSchemaExist(schemaList, schema);
			if( checkSchemaExist )
			{
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Hive connection successfully established.");
			}
			else
			{
				throw new IllegalArgumentException("Schema '" + schema + "' not found in datasource '" + ds.getId() + "'");
			}
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownSchema").message(iae.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownSchema").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}

	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = getConnectionParam(ds, PARAM_SCHEMA_ID);
		try
		{
			connection = getDataBaseConnection(ds);
			metadata = metadataByConnection(connection, databaseName, ds.getId());
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownSchema").message(iae.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return metadata;
	}

	private Connection getDataBaseConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String port = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_SCHEMA_ID);
		return getDataBaseConnection(hostName, port, databaseName, userName, password);
	}

	private Connection getDataBaseConnection(String hostName, String port, String databaseName, String userName, String password) throws SQLException, ClassNotFoundException
	{
		Connection connection = null;
		String driver = "org.apache.hive.jdbc.HiveDriver";
		Class.forName(driver);
		String url = "jdbc:hive2://" + hostName + ":" + port + "/" + databaseName;
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection con, String schema, String dataSourceId) throws DataExtractionServiceException
	{
		Metadata metadata = new Metadata();
		List<String> schemaList = null;
		boolean checkSchemaExist = false;
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		try
		{
			schemaList = getScheamsFromDatabase(con);

			if( schemaList != null && schemaList.size() == 0 )
			{
				throw new IllegalArgumentException("No schema's found in '" + dataSourceId + "' dataSource.");
			}

			checkSchemaExist = checkSchemaExist(schemaList, schema);

			if( checkSchemaExist )
			{
				List<NamedType> tableList = getSchemaRelatedTables(con, schema);
				for (NamedType namedType : tableList)
				{
					/* table entry type */
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);

					List<NamedType> attributeListForColumns = getTableRelatedColumns(con, schema, namedType.getName());
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
			else
			{
				throw new IllegalArgumentException("Schema '" + schema + "' not found in datasource '" + dataSourceId + "'");
			}

		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	@SuppressWarnings("unused")
	private List<Constraint> getTableRelatedPkInfo(Connection con, String schema, String tableName) throws DataExtractionServiceException
	{
		DatabaseMetaData databaseMetaData;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		ResultSet primaryKeysResultSet = null;
		try
		{
			databaseMetaData = con.getMetaData();
			primaryKeysResultSet = databaseMetaData.getPrimaryKeys(null, schema, tableName.split("\\.")[1]);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (primaryKeysResultSet.next())
			{
				pkAttributes.add(primaryKeysResultSet.getString("PK_NAME"));
			}
			if( pkAttributes != null && pkAttributes.size() > 0 )
			{
				constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
				constraint.setAttributes(pkAttributes);
				constraintList.add(constraint);
			}
		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(primaryKeysResultSet);
		}
		return constraintList;
	}

	@SuppressWarnings("unused")
	private List<Constraint> getTableRelatedFkInfo(Connection con, String schema, String tableName) throws DataExtractionServiceException
	{
		List<Constraint> constraintList = new ArrayList<Constraint>();
		ResultSet primaryKeysResultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = con.getMetaData();
			primaryKeysResultSet = databaseMetaData.getImportedKeys(null, schema, tableName.split("\\.")[1]);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (primaryKeysResultSet.next())
			{
				pkAttributes.add(primaryKeysResultSet.getString("referenced_column_name"));
			}
			if( pkAttributes != null && pkAttributes.size() > 0 )
			{
				constraint.kind(Constraint.KindEnum.FOREIGN_KEY);
				constraint.setAttributes(pkAttributes);
				constraintList.add(constraint);
			}
		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(primaryKeysResultSet);
		}
		return constraintList;
	}

	private List<NamedType> getSchemaRelatedTables(Connection con, String dataSource) throws DataExtractionServiceException
	{
		DatabaseMetaData databaseMetaData;
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;
		try
		{
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getTables(null, dataSource, "%", null);
			while (resultSet.next())
			{
				NamedType namedType = new NamedType();
				namedType.setName(dataSource + "." + resultSet.getString(3));
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(resultSet);
		}
		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(Connection con, String schemaName, String tableName) throws DataExtractionServiceException
	{
		DatabaseMetaData databaseMetaData;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		ResultSet resultSet = null;
		try
		{
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getColumns(null, schemaName, tableName.split("\\.")[1], null);
			while (resultSet.next())
			{
				String type = resultSet.getString("TYPE_NAME");
				String columnName = resultSet.getString("COLUMN_NAME");
				if( type.contains("array") )
				{
					if( type.contains("struct") )
					{
						String modifiedType = removeParenthesis(type.replace("array", "").replace("struct", "").replace("<", "").replace(">", ""), "()");
						String[] columnNamesWithTypes = modifiedType.split(",");
						List<String> list = Arrays.asList(columnNamesWithTypes);
						NamedType namedType = new NamedType();
						namedType.setName(columnName);
						namedType.setType(new Type().kind(Type.KindEnum.LIST));
						List<NamedType> attributeListForColumns = new ArrayList<>();
						Type entryType = new Type().kind(Type.KindEnum.OBJECT);
						namedType.getType().setEntryType(entryType);
						for (String columnType : list)
						{
							String[] columnAndDataType = columnType.split(":");
							NamedType attributeForColumn = new NamedType();
							attributeForColumn.setName(columnAndDataType[0]);
							NamedType childNamedType = new NamedType();
							childNamedType.setName(columnAndDataType[0]);
							Type childCloumnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(columnAndDataType[1].toUpperCase().trim())).nullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false).autoIncrement(resultSet.getMetaData().isAutoIncrement(1) ? true : false);
							childNamedType.setType(childCloumnType);
							attributeListForColumns.add(childNamedType);
						}
						entryType.setAttributes(attributeListForColumns);
						attributeList.add(namedType);
					}
				}
				else if( type.contains("string") )
				{
					String modifiedType = type.replace("array", "").replace("<", "").replace(">", "");
					NamedType attributeForColumn = new NamedType();
					attributeForColumn.setName(columnName);
					Type typeForCoumn = new Type().kind(Type.KindEnum.LIST).dataType(getDataType(modifiedType.toUpperCase())).nullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false).autoIncrement(resultSet.getMetaData().isAutoIncrement(1) ? true : false);
					attributeForColumn.getType().setEntryType(typeForCoumn);
					attributeList.add(attributeForColumn);
				}
				else
				{
					NamedType attributeForColumn = new NamedType();
					attributeForColumn.setName(columnName);
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type.toUpperCase())).nullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false).autoIncrement(resultSet.getMetaData().isAutoIncrement(1) ? true : false);
					attributeForColumn.setType(typeForCoumn);
					attributeList.add(attributeForColumn);
				}
			}
		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(resultSet);
		}

		return attributeList;
	}

	private List<String> getScheamsFromDatabase(Connection con) throws SQLException
	{
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		try
		{
			if( con != null )
			{
				DatabaseMetaData meta = con.getMetaData();
				resultSet = meta.getSchemas();
				while (resultSet.next())
				{
					String schema = resultSet.getString("TABLE_SCHEM");
					if( isWantedSchema(schema) )
					{
						schemaList.add(schema);
					}
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		}
		finally
		{
			closeSqlObject(resultSet);
		}
		return schemaList;
	}

	private boolean checkSchemaExist(List<String> schemaList, String schema) throws SQLException
	{
		boolean checkSchemaExist = false;

		if( schemaList != null && schemaList.size() > 0 )
		{
			if( StringUtils.isNotBlank(schema) )
			{

				for (String schemaName : schemaList)
				{
					if( schema.equals(schemaName) )
					{
						checkSchemaExist = true;
					}
				}
			}
		}
		return checkSchemaExist;
	}

	private boolean isWantedSchema(String schemaName)
	{
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
		defaultSchemas = new String[] { "master", "tempdb", "msdb", "model" };
		boolean wantedSchema = true;
		for (int i = 0; i < defaultSchemas.length; i++)
		{
			defaultSchemasList.add(defaultSchemas[i]);
		}
		if( defaultSchemasList.contains(schemaName) )
		{
			wantedSchema = false;
		}
		return wantedSchema;
	}

	@Override
	public int getCountRows(DataSource ds,DataExtractionSpec spec) throws DataExtractionServiceException
	{

		int countRows = 0;
		Connection connection = null;

		try
		{
			String tableName = spec.getCollection();
			connection = getDataBaseConnection(ds);
			String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
			String[] schemaTable = parse(tableName).segments();
			if( schemaTable.length == 2 )
			{
				schemaName = getNameByPattern(schemaTable[0], "`", "`");
				tableName = getNameByPattern(schemaTable[1], "`", "`");
			}
			else
			{
				throw new IllegalArgumentException("Unknown collection '" + tableName + "'");
			}
			countRows = getObjectCount(connection, schemaName + "." + tableName);
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownCollection").message(e.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return countRows;
	}

	/*
	 * removing parenthesis and everything inside them, works for (),[] and {}
	 */

	private String removeParenthesis(String input_string, String parenthesis_symbol)
	{
		if( parenthesis_symbol.contains("[]") )
		{
			return input_string.replaceAll("\\s*\\[[^\\]]*\\]\\s*", " ");
		}
		else if( parenthesis_symbol.contains("{}") )
		{
			return input_string.replaceAll("\\s*\\{[^\\}]*\\}\\s*", " ");
		}
		else
		{
			return input_string.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ");
		}
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
			final DataExtractionThread dataExtractionExecutor = new HiveDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class HiveDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public HiveDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

				String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);

				String tableName = spec.getCollection();

				String[] schemaTable = parse(tableName).segments();
				if( schemaTable.length == 2 )
				{
					schemaName = schemaTable[0].replaceAll(" ", "\\ ");
					tableName = schemaTable[1].replaceAll(" ", "\\ ");
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownCollection").message("Unknown collection '" + tableName + "'"));
				}

				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,

						dataSourcePassword, tableName, getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "),

						dataSourceHost, dataSourcePort,

						schemaName, job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

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
		private Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceUser,

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