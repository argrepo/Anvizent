package com.prifender.des.adapter.relational.postgresql;

import static com.prifender.des.CollectionName.fromSegments;
import static com.prifender.des.CollectionName.parse;
import static com.prifender.des.util.DatabaseUtil.closeSqlObject;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getObjectCount;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
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
 * The PostgreSQLDataSourceAdapter Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch Relational metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-05-17
 */
@Component
public final class PostgreSQLDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final String JOB_NAME = "local_project.prifender_postgresql_m3_v1_0_1.Prifender_PostgreSQL_M3_v1";

	private static final String DEPENDENCY_JAR = "prifender_postgresql_m3_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	public static final String TYPE_ID = "PostgreSQL";
	public static final String TYPE_LABEL = "PostgreSQL";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(clone(PARAM_PORT).required(false).defaultValue("5432")).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD)
			.addConnectionParamsItem(PARAM_DATABASE).addConnectionParamsItem(clone(PARAM_SCHEMA).required(false));

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
			final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
			final String schema = getConnectionParam(ds, PARAM_SCHEMA_ID);
			connection = getDataBaseConnection(ds);
			if( connection != null )
			{
				List<String> schemaList = null;

				if( StringUtils.isNotBlank(databaseName) && StringUtils.isBlank(schema) )
				{
					schemaList = getAllSchemasFromDatabase(connection);
				}

				if( StringUtils.isNotBlank(databaseName) && StringUtils.isNotBlank(schema) )
				{
					List<String> schemaListFromDb = getAllSchemasFromDatabase(connection);

					if( schemaListFromDb != null && schemaListFromDb.size() > 0 )
					{
						for (String schemaname : schemaListFromDb)
						{
							if( schema.equals(schemaname) )
							{
								schemaList.add(schema);
							}
						}
						if( schemaList == null )
						{
							throw new IllegalArgumentException("Schema '" + schema + "' not found in '" + databaseName + "' database.");
						}
					}
					else
					{
						throw new IllegalArgumentException("no schema's found in '" + databaseName + "' database.");
					}
				}
				if( schemaList == null )
				{
					throw new IllegalArgumentException("no schema's found in '" + databaseName + "' database.");
				}
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("PostgreSQL connection successfully established.");
			}
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to PostgreSQL database.");
	}

	/**
	 * @param ds
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws DataExtractionServiceException
	 */
	private Connection getDataBaseConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{

		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}

		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String port = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		return getDataBaseConnection(host, port, databaseName, userName, password);
	}

	/**
	 * @param endPoint
	 * @param port
	 * @param databaseName
	 * @param userName
	 * @param password
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private Connection getDataBaseConnection(String hostName, String port, String databaseName, String userName, String password) throws SQLException, ClassNotFoundException
	{
		Connection connection = null;
		String driver = "org.postgresql.Driver";
		String url = "jdbc:postgresql://" + hostName + ":" + port + (databaseName != null ? "/" + databaseName : "");
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Connection connection = null;
		try
		{
			final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
			final String schema = getConnectionParam(ds, PARAM_SCHEMA_ID);
			connection = getDataBaseConnection(ds);
		    metadata = metadataByConnection(connection, databaseName, schema);
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return metadata;

	}

	/**
	 * @param connection
	 * @param databaseName
	 * @return
	 * @throws SQLException
	 * @throws DataExtractionServiceException
	 */
	private Metadata metadataByConnection(Connection connection, String databaseName, String schema) throws SQLException
	{

		Metadata metadata = new Metadata();
		List<String> schemaList = null;

		if( StringUtils.isNotBlank(databaseName) && StringUtils.isBlank(schema) )
		{
			schemaList = getAllSchemasFromDatabase(connection);
		}

		if( StringUtils.isNotBlank(databaseName) && StringUtils.isNotBlank(schema) )
		{
			List<String> schemaListFromDb = getAllSchemasFromDatabase(connection);

			if( schemaListFromDb != null && schemaListFromDb.size() > 0 )
			{
				for (String schemaname : schemaListFromDb)
				{
					if( schema.equals(schemaname) )
					{
						schemaList.add(schema);
					}
				}
				if( schemaList == null )
				{
					throw new IllegalArgumentException("Schema '" + schema + "' not found in '" + databaseName + "' database.");
				}
			}
			else
			{
				throw new IllegalArgumentException("no schema's found in '" + databaseName + "' database.");
			}
		}
		if( schemaList == null )
		{
			throw new IllegalArgumentException("no schema's found in '" + databaseName + "' database.");
		}

		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (String schemaName : schemaList)
		{
			List<NamedType> tableList = getDatasourceRelatedTables(connection, databaseName, schemaName);
			for (NamedType namedType : tableList)
			{
				/* table entry type */
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				String tableName = namedType.getName();

				List<NamedType> attributeListForColumns = getTableRelatedColumns(connection, databaseName, schemaName, tableName);
				entryType.setAttributes(attributeListForColumns);

				/* added primary keys here */
				List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
				List<Constraint> pkConstraintList = getTableRelatedPkInfo(connection, databaseName, schemaName, tableName);
				if( pkConstraintList != null && pkConstraintList.size() > 0 )
				{
					for (Constraint constraint : pkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				/* add foreign keys here */
				List<Constraint> fkConstraintList = getTableRelatedFkInfo(connection, databaseName, schemaName, tableName);
				if( fkConstraintList != null )
				{
					for (Constraint constraint : fkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				entryType.setConstraints(pkFkConstraintList);
				namedType.setName(fromSegments( databaseName,schemaName,namedType.getName()).toString());
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;

	}

	/**
	 * 
	 * @param con
	 * @param dataSource
	 * @param schemaName
	 * @param tableName
	 * @return
	 */
	private List<Constraint> getTableRelatedPkInfo(Connection con, String dataSource, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		DatabaseMetaData databaseMetaData = null;
		try
		{
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getPrimaryKeys(dataSource, schemaName, tableName);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (resultSet.next())
			{
				boolean isPrimarykey = resultSet.getString("COLUMN_NAME") != null ? true : false;
				if( StringUtils.isNotBlank(resultSet.getString("COLUMN_NAME")) )
				{
					if( isPrimarykey )
					{
						pkAttributes.add(resultSet.getString("COLUMN_NAME"));
					}
				}
			}
			if( pkAttributes != null && pkAttributes.size() > 0 )
			{
				constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
			}
			constraint.setAttributes(pkAttributes);
			constraintList.add(constraint);
		}
		finally
		{
			closeSqlObject(resultSet, preparedStatement);
		}
		return constraintList;
	}

	/**
	 * 
	 * @param con
	 * @param databaseName
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 */
	private List<NamedType> getDatasourceRelatedTables(Connection con, String databaseName, String schemaName) throws SQLException
	{
		DatabaseMetaData databaseMetaData = null;
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;

		try
		{
			databaseMetaData = con.getMetaData();
			String[] types = { "TABLE" };
			resultSet = databaseMetaData.getTables(databaseName, schemaName, "%", types);
			while (resultSet.next())
			{
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString("TABLE_NAME"));
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}

			Collections.sort(tableList, new Comparator<NamedType>()
			{
				public int compare(NamedType result1, NamedType result2)
				{
					return result1.getName().compareToIgnoreCase(result2.getName());
				}
			});
		}
		finally
		{
			closeSqlObject(resultSet);
		}

		return tableList;
	}

	/**
	 * 
	 * @param con
	 * @return
	 * @throws SQLException
	 */
	public List<String> getAllSchemasFromDatabase(Connection con) throws SQLException
	{
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
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
			closeSqlObject(resultSet, preparedStatement);
		}
		return schemaList;
	}

	public boolean isWantedSchema(String schemaName)
	{
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
		defaultSchemas = new String[] { "information_schema", "pg_catalog", "pg_internal" };
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

	/**
	 * @param con
	 * @param database
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 * @throws DataExtractionServiceException
	 */
	private List<NamedType> getTableRelatedColumns(Connection con, String database, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		DatabaseMetaData databaseMetaData;
		databaseMetaData = con.getMetaData();
		List<NamedType> attributeList = new ArrayList<NamedType>();

		try
		{
			resultSet = databaseMetaData.getColumns(database, schemaName, tableName, null);
			while (resultSet.next())
			{
				String type = resultSet.getString("TYPE_NAME");
				String columnName = resultSet.getString("COLUMN_NAME");
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(columnName);
				Type columnInfo = getColumnInfo(con, database, schemaName, tableName, columnName);
				if( columnInfo != null )
				{
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type.toUpperCase())).nullable(columnInfo.getNullable()).autoIncrement(columnInfo.getAutoIncrement()).size(columnInfo.getSize());
					attributeForColumn.setType(typeForCoumn);
					attributeList.add(attributeForColumn);
				}
			}
		}
		finally
		{
			closeSqlObject(resultSet, preparedStatement);
		}

		return attributeList;
	}

	/**
	 * @param con
	 * @param database
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	private List<Constraint> getTableRelatedFkInfo(Connection con, String database, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try
		{
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getImportedKeys(database, schemaName, tableName);
			while (resultSet.next())
			{
				boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
				List<String> fkAttributes = new ArrayList<>();
				List<String> pkOfFkAttributes = new ArrayList<>();
				Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
				if( StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME")) )
				{
					if( isForeignKey )
					{
						fkAttributes.add(resultSet.getString("PKCOLUMN_NAME"));
						pkOfFkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
						constraint.setTarget(database + "." + resultSet.getString("FKTABLE_SCHEM") + "." + resultSet.getString("PKTABLE_NAME"));
					}
				}
				constraint.setAttributes(pkOfFkAttributes);
				constraint.setTargetAttributes(fkAttributes);
				constraintList.add(constraint);
			}
		}
		finally
		{
			closeSqlObject(resultSet, preparedStatement);
		}

		return constraintList;
	}

	/**
	 * @param connection
	 * @param dataBaseName
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private Type getColumnInfo(Connection connection, String dataBaseName, String schemaName, String tableName, String columnName) throws SQLException
	{
		Type type = new Type();
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData meta = connection.getMetaData();
			resultSet = meta.getColumns(dataBaseName, schemaName, tableName, columnName);
			if( resultSet.next() )
			{
				type.setSize((resultSet.getInt("COLUMN_SIZE")));
				type.setAutoIncrement(resultSet.getString("IS_AUTOINCREMENT").equals("YES") ? true : false);
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false);
			}
		}
		finally
		{
			closeSqlObject(resultSet);
		}
		return type;
	}

	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		Connection connection = null;
		try
		{
			String tableName = spec.getCollection();
			connection = getDataBaseConnection(ds);
			String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
			String[] schemaTable = parse(tableName).segments();
			if( schemaTable.length == 3 )
			{
				schemaName = schemaTable[1];
				tableName = schemaTable[2];
			}
			else
			{
				throw new IllegalArgumentException("schema is null.");
			}
			countRows = getObjectCount(connection, "\"" + schemaName + "\"" + "." + "\"" + tableName + "\"");
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

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName, TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new PostgreSQLDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class PostgreSQLDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public PostgreSQLDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

				DataExtractionJob job, int objectCount, String adapterHome, int containersCount)

				throws DataExtractionServiceException
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

				String schemaName = "";

				String[] schemaTable = StringUtils.split(tableName, "\\.");

				if( schemaTable.length == 3 )
				{
					databaseName = schemaTable[0].replaceAll(" ", "\\ ");

					schemaName = schemaTable[1].replaceAll(" ", "\\ ");

					tableName = schemaTable[2].replaceAll(" ", "\\ ");

				}
				if( schemaTable.length == 2 )
				{
					schemaName = schemaTable[0].replaceAll(" ", "\\ ");

					tableName = schemaTable[1].replaceAll(" ", "\\ ");
				}
				if( databaseName == null )
				{
					throw new IllegalArgumentException("database is null ");
				}
				String dataSourceTableName = null;

				if( SystemUtils.IS_OS_WINDOWS )
				{
					dataSourceTableName = "\\\"" + schemaName + "\\\"" + "." + "\\\"" + tableName + "\\\"";

				}
				else
				{
					dataSourceTableName = "\"" + schemaName + "\"" + "." + "\"" + tableName + "\"";
				}

				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,

						dataSourcePassword, dataSourceTableName, getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), dataSourceHost,

						dataSourcePort, databaseName, job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

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
