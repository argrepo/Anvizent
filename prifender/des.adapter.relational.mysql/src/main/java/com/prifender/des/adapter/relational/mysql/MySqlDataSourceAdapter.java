package com.prifender.des.adapter.relational.mysql;

import static com.prifender.des.util.DatabaseUtil.closeSqlObject;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getObjectCount;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import static com.prifender.des.CollectionName.fromSegments;
import static com.prifender.des.CollectionName.parse;
import static com.prifender.des.util.DatabaseUtil.getNameByPattern;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
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
 * The MySqlDataSourceAdapter Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch relational metadata based on datasource
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
public final class MySqlDataSourceAdapter extends DataSourceAdapter
{

	private static final String JOB_NAME = "local_project.prifender_mysql_m2_v1_0_1.Prifender_MYSQL_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_mysql_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "MySql";
	public static final String TYPE_LABEL = "MySQL";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(clone(PARAM_PORT).required(false).defaultValue("3306")).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD)
			.addConnectionParamsItem(PARAM_DATABASE);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException
	{
		Connection connection = null;
		List<String> databaseList = null;
		boolean checkDbStatus = false;
		try
		{
			final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
			connection = getConnection(ds);
			if( connection != null )
			{
				databaseList = getAllDatasourcesFromDatabase(connection);
				if( databaseList == null )
				{
					throw new IllegalArgumentException("No Databases found in datasource '" + ds.getId() + "'");
				}
				if( StringUtils.isNotBlank(databaseName) )
				{
					for (String dbName : databaseList)
					{
						if( databaseName.equals(dbName) )
						{
							checkDbStatus = true;
						}
					}
				}
				if( checkDbStatus )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Mysql connection successfully established.");
				}
				else
				{
					throw new IllegalArgumentException("Database '" + databaseName + "' not found in datasource '" + ds.getId() + "'");
				}

			}
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		catch ( IllegalArgumentException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to mysql database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		try
		{
			connection = getConnection(ds);

			if( connection != null )
			{
				metadata = metadataByConnection(connection, databaseName, ds.getId());
			}
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDatabases").message(e.getMessage()));
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

	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		Connection connection = null;
		try
		{
			String tableName = spec.getCollection();
			connection = getConnection(ds);
			String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
			String[] databaseTable = parse(tableName).segments();
			if( databaseTable.length == 2 )
			{
				databaseName = getNameByPattern(databaseTable[0], "`", null);
				tableName = getNameByPattern(databaseTable[1], "`", null);
			}
			else
			{
				throw new IllegalArgumentException("Unknown collection '" + tableName + "'");
			}
			countRows = getObjectCount(connection, databaseName + "." + tableName);
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return countRows;
	}

	private Connection getConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}
		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		return getConnection(hostName, portNumber, userName, password, databaseName);
	}

	private Connection getConnection(String hostName, String portNumber, String userName, String password, String databaseName) throws SQLException, ClassNotFoundException
	{
		Connection connection = null;
		String driver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://" + hostName + ":" + portNumber + (databaseName != null ? "/" + databaseName : "");
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection connection, String databaseName, String dsId) throws SQLException, DataExtractionServiceException
	{
		boolean checkDbStatus = false;
		Metadata metadata = new Metadata();
		List<String> databaseList = null;
		List<String> dataSourceList = getAllDatasourcesFromDatabase(connection);

		if( dataSourceList == null )
		{
			throw new IllegalArgumentException("No Databases found in datasource '" + dsId + "'");
		}

		if( StringUtils.isNotBlank(databaseName) )
		{
			for (String dbName : dataSourceList)
			{
				if( databaseName.equals(dbName) )
				{
					checkDbStatus = true;
				}
			}
		}
		if( !checkDbStatus )
		{
			throw new IllegalArgumentException("Database '" + databaseName + "' not found in datasource '" + dsId + "'");
		}
		else
		{
			databaseList = new ArrayList<String>();
			databaseList.add(databaseName);
		}

		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (String dataSource : databaseList)
		{
			List<NamedType> tableList = getDatasourceRelatedTables(connection, dataSource);
			for (NamedType namedType : tableList)
			{
				/* table entry type */
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getTableRelatedColumns(connection, dataSource, namedType.getName());
				entryType.setAttributes(attributeListForColumns);

				/* add primary keys here */
				List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
				List<Constraint> pkConstraintList = getTableRelatedPrimarykeyInfo(connection, dataSource, namedType.getName());
				if( pkConstraintList != null )
				{
					for (Constraint constraint : pkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				/* add foreign keys here */
				List<Constraint> fkConstraintList = getTableRelatedForeignkeyInfo(connection, dataSource, namedType.getName());
				if( fkConstraintList != null )
				{
					for (Constraint constraint : fkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				entryType.setConstraints(pkFkConstraintList);
				namedType.setName(fromSegments(dataSource, namedType.getName()).toString());
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	private List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException
	{
		List<NamedType> tableList = new ArrayList<>();
		PreparedStatement prepareStatement = null;
		ResultSet resultSet = null;
		try
		{
			String tablesQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE in('BASE TABLE','VIEW') AND TABLE_SCHEMA=? order by TABLE_NAME";
			prepareStatement = con.prepareStatement(tablesQuery);
			prepareStatement.setString(1, schemaName);
			resultSet = prepareStatement.executeQuery();
			while (resultSet.next())
			{
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString(1));
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
			closeSqlObject(resultSet, prepareStatement);
		}
		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(Connection con, String schemaName, String tableName) throws SQLException, DataExtractionServiceException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		String columnsQuery = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{
			columnsQuery = "SELECT COLUMN_NAME,DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=? ";
			preparedStatement = con.prepareStatement(columnsQuery);
			preparedStatement.setString(1, tableName);
			preparedStatement.setString(2, schemaName);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
			{
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(resultSet.getString(1));
				Type columnInfo = getColumnInfo(con, schemaName, tableName, resultSet.getString(1));
				if( columnInfo != null )
				{
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(resultSet.getString(2).toUpperCase())).nullable(columnInfo.getNullable()).autoIncrement(columnInfo.getAutoIncrement()).size(columnInfo.getSize());
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

	private Type getColumnInfo(Connection connection, String dataBaseName, String tableName, String columnName) throws DataExtractionServiceException
	{
		Type type = new Type();
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData meta = connection.getMetaData();
			resultSet = meta.getColumns(null, dataBaseName, tableName, columnName);
			if( resultSet.next() )
			{
				type.setSize((resultSet.getInt("COLUMN_SIZE")));
				type.setAutoIncrement(resultSet.getString("IS_AUTOINCREMENT").equals("YES") ? true : false);
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false);
			}
		}
		catch ( SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}
		finally
		{

			closeSqlObject(resultSet);
		}
		return type;
	}

	private List<Constraint> getTableRelatedForeignkeyInfo(Connection con, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try
		{
			String columnsQuery = "SELECT  kcu.COLUMN_NAME  AS COLUMN_NAME, rc.UPDATE_RULE     AS ON_UPDATE," + "  rc.DELETE_RULE    AS ON_DELETE, kcu.REFERENCED_TABLE_NAME   AS TARGET_TABLE, kcu.REFERENCED_COLUMN_NAME   AS TARGET_COLUMN " + " FROM "
					+ " information_schema.KEY_COLUMN_USAGE     kcu   " + " INNER JOIN  information_schema.REFERENTIAL_CONSTRAINTS  rc    " + " ON (rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND rc.TABLE_NAME = kcu.TABLE_NAME AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME)  "
					+ " WHERE       kcu.TABLE_SCHEMA   = ?  AND  kcu.REFERENCED_TABLE_SCHEMA = ? AND   kcu.TABLE_NAME   = ? ";
			preparedStatement = con.prepareStatement(columnsQuery);
			preparedStatement.setString(1, schemaName);
			preparedStatement.setString(2, schemaName);
			preparedStatement.setString(3, tableName);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
			{
				List<String> fkAttributes = new ArrayList<>();
				List<String> pkOfFkAttributes = new ArrayList<>();
				pkOfFkAttributes.add(resultSet.getString(1));
				Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
				fkAttributes.add(resultSet.getString(5));
				constraint.setTarget(schemaName + "." + resultSet.getString(4));
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

	private List<Constraint> getTableRelatedPrimarykeyInfo(Connection con, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try
		{
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getPrimaryKeys(schemaName, null, tableName);
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
			closeSqlObject(resultSet);
		}
		return constraintList;
	}

	private List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException
	{
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		Statement statement = null;
		try
		{
			if( con != null )
			{
				String sql = "select schema_name from information_schema.SCHEMATA where schema_name not in('information_schema') order by schema_name";
				statement = con.createStatement();
				resultSet = statement.executeQuery(sql);
				while (resultSet.next())
				{
					schemaList.add(resultSet.getString(1));
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);
		}
		finally
		{
			closeSqlObject(resultSet, statement);
		}
		return schemaList;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, final int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_LABEL);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName, TYPE_LABEL, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new MySQLDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class MySQLDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public MySQLDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

				String[] databaseTable = parse(tableName).segments();

				if( databaseTable.length == 2 )
				{
					databaseName = databaseTable[0].replaceAll(" ", "\\ ");
					tableName = databaseTable[1].replaceAll(" ", "\\ ");
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
