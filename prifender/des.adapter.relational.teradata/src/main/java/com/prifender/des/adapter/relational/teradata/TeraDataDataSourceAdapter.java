package com.prifender.des.adapter.relational.teradata;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
 * The TeraDataDataSourceAdapter Component implements
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
public final class TeraDataDataSourceAdapter extends DataSourceAdapter
{

	private static final String JOB_NAME = "local_project.prifender_teradata_m2_v1_0_1.Prifender_Teradata_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_teradata_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	public static final String TYPE_ID = "Teradata";
	public static final String TYPE_LABEL = "Teradata";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(PARAM_PORT).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD).addConnectionParamsItem(PARAM_DATABASE);

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
		boolean checkDatabaseExist = false;
		try
		{
			connection = getDataBaseConnection(ds);
			if( connection != null )
			{
				databaseList = getAllDatabasesFromDb(connection);
				if( databaseList == null )
				{
					throw new IllegalArgumentException("No databases's found in '" + ds.getId() + "' dataSource.");
				}
				final String database = getConnectionParam(ds, PARAM_DATABASE_ID);
				checkDatabaseExist = checkDatabaseExist(databaseList, database);

				if( checkDatabaseExist )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Teradata connection successfully established.");
				}
				else
				{
					throw new IllegalArgumentException("Database '" + database + "' not found in datasource '" + ds.getId() + "'");
				}
			}
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDatabase").message(iae.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Teradata");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);

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
			throw new DataExtractionServiceException(new Problem().code("unknownDatabase").message(iae.getMessage()));
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

	private Connection getDataBaseConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDatasource").message("datasource is null"));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);

		return getDataBaseConnection(hostName, portNumber, databaseName, userName, password);
	}

	private Connection getDataBaseConnection(String hostName, String portNumber, String databaseName, String userName, String password) throws SQLException, ClassNotFoundException
	{
		Connection connection = null;
		String driver = "com.teradata.jdbc.TeraDriver";
		String url = "jdbc:teradata://" + hostName + "/" + (databaseName != null ? "database=" + databaseName : "");
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection con, String database, String dataSourceId) throws DataExtractionServiceException
	{
		Metadata metadata = new Metadata();
		List<String> databaseList = null;
		boolean checkDatabaseExist = false;
		try
		{
			databaseList = getAllDatabasesFromDb(con);
			if( databaseList == null )
			{
				throw new IllegalArgumentException("No databases's found in '" + dataSourceId + "' dataSource.");
			}
			checkDatabaseExist = checkDatabaseExist(databaseList, database);

			if( checkDatabaseExist )
			{
				List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
				// for (String dataSource : databaseList)
				// {
				List<NamedType> tableList = getDatabaseRelatedTables(con, database);
				for (NamedType namedType : tableList)
				{
					/* table entry type */
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);

					List<NamedType> attributeListForColumns = getTableRelatedColumns(con, database, namedType.getName());
					entryType.setAttributes(attributeListForColumns);

					/* add primary keys here */
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList = getTableRelatedPkInfo(con, database, namedType.getName());
					if( pkConstraintList != null )
					{
						for (Constraint constraint : pkConstraintList)
						{
							pkFkConstraintList.add(constraint);
						}
					}
					/* add foreign keys here */
					List<Constraint> fkConstraintList = getTableRelatedFkInfo(con, database, namedType.getName());
					if( fkConstraintList != null )
					{
						for (Constraint constraint : fkConstraintList)
						{
							pkFkConstraintList.add(constraint);
						}
					}
					entryType.setConstraints(pkFkConstraintList);
					namedType.setName(database + "." + namedType.getName());
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
			// }
			else
			{
				throw new IllegalArgumentException("Database '" + database + "' not found in datasource '" + dataSourceId + "'");
			}
		}
		catch ( DataExtractionServiceException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	private List<NamedType> getDatabaseRelatedTables(Connection con, String dataBase) throws DataExtractionServiceException
	{
		DatabaseMetaData databaseMetaData;
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;
		try
		{
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getTables(null, dataBase, "%", null);
			while (resultSet.next())
			{
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString("TABLE_NAME"));
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

	private List<NamedType> getTableRelatedColumns(Connection con, String dataBase, String tableName) throws DataExtractionServiceException
	{
		DatabaseMetaData databaseMetaData;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		ResultSet resultSet = null;
		try
		{
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getColumns(null, dataBase, tableName, null);
			while (resultSet.next())
			{
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(resultSet.getString("COLUMN_NAME"));
				Type columnInfo = getColumnInfo(con, dataBase, tableName, resultSet.getString("COLUMN_NAME"));
				if( columnInfo != null )
				{
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(resultSet.getString("TYPE_NAME").toUpperCase())).nullable(columnInfo.getNullable()).autoIncrement(columnInfo.getAutoIncrement()).size(columnInfo.getSize());
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
				type.setSize(resultSet.getInt("COLUMN_SIZE"));
				type.setAutoIncrement(resultSet.getMetaData().isAutoIncrement(1) ? true : false);
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false);
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
		return type;
	}

	private List<Constraint> getTableRelatedPkInfo(Connection con, String database, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try
		{
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getPrimaryKeys(null, database, tableName);
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

	private List<Constraint> getTableRelatedFkInfo(Connection con, String database, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		List<Constraint> constraintList = new ArrayList<>();
		try
		{
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getImportedKeys(null, database, tableName);
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
						constraint.setTarget(database + "." + resultSet.getString("PKTABLE_NAME"));
					}
				}
				constraint.setAttributes(pkOfFkAttributes);
				constraint.setTargetAttributes(fkAttributes);
				constraintList.add(constraint);
			}
		}
		finally
		{
			closeSqlObject(resultSet);
		}

		return constraintList;
	}

	private List<String> getAllDatabasesFromDb(Connection con) throws SQLException
	{
		List<String> databaseList = new ArrayList<>();
		ResultSet resultSet = null;
		try
		{
			if( con != null )
			{
				DatabaseMetaData dm = con.getMetaData();
				ResultSet rs = dm.getSchemas();
				while (rs.next())
				{
					databaseList.add(rs.getString("TABLE_SCHEM"));
				}
			}
			Collections.sort(databaseList, String.CASE_INSENSITIVE_ORDER);

		}
		finally
		{
			closeSqlObject(resultSet);
		}
		return databaseList;
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
			if( schemaTable.length == 2 )
			{
				schemaName = getNameByPattern(schemaTable[0], "\"", null);
				tableName = getNameByPattern(schemaTable[1], "\"", null);
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

	private boolean checkDatabaseExist(List<String> schemaList, String schema) throws SQLException
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

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName, TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new TeraDataDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class TeraDataDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public TeraDataDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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