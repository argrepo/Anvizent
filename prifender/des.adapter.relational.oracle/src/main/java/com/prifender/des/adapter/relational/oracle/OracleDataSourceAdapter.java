package com.prifender.des.adapter.relational.oracle;

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
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
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
 * The OracleDataSourceAdapter Component implements
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
public final class OracleDataSourceAdapter extends DataSourceAdapter
{

	private static final String JOB_NAME = "local_project.prifender_oracle_m2_v1_0_1.Prifender_Oracle_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_oracle_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	private static final String TYPE_ID = "Oracle";
	public static final String TYPE_LABEL = "Oracle";

	// ServiceName

	public static final String PARAM_SERVICE_NAME_ID = "ServiceName";
	public static final String PARAM_SERVICE_NAME_LABEL = "Service Name";
	public static final String PARAM_SERVICE_NAME_DESCRIPTION = "An alias of an Oracle instance on the specified host";

	public static final ConnectionParamDef PARAM_SERVICE_NAME = new ConnectionParamDef().id(PARAM_SERVICE_NAME_ID).label(PARAM_SERVICE_NAME_LABEL).description(PARAM_SERVICE_NAME_DESCRIPTION).type(TypeEnum.STRING).required(false);

	// SID

	public static final String PARAM_SID_ID = "SID";
	public static final String PARAM_SID_LABEL = "SID";
	public static final String PARAM_SID_DESCRIPTION = "The name of the Oracle instance on the specified host";

	public static final ConnectionParamDef PARAM_SID = new ConnectionParamDef().id(PARAM_SID_ID).label(PARAM_SID_LABEL).description(PARAM_SID_DESCRIPTION).type(TypeEnum.STRING).required(false);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(clone(PARAM_PORT).required(false).defaultValue("1521")).addConnectionParamsItem(PARAM_SERVICE_NAME).addConnectionParamsItem(PARAM_SID)
			.addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PASSWORD).addConnectionParamsItem(clone(PARAM_SCHEMA).required(false));

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException
	{
		Connection connection = null;
		boolean checkDbStatus = false;
		List<String> schemaList = null;
		try
		{
			String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
			connection = getConnection(ds);
			if( connection != null )
			{
				schemaList = getAllDatasourcesFromDatabase(connection);

				if( schemaList.size() == 0 )
				{
					throw new IllegalArgumentException("No schemas found in '" + ds.getId() + "' dataSource.");
				}

				if( schemaName != null )
				{

					for (String schema : schemaList)
					{
						if( schema.equals(schemaName) )
						{
							checkDbStatus = true;
						}

					}
					if( checkDbStatus )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("oracle connection successfully established.");
					}
					else
					{
						throw new IllegalArgumentException("Schema '" + schemaName + "' not found in '" + ds.getId() + "' dataSource.");
					}
				}
				else
				{
					if( schemaList.size() > 0 )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("oracle connection successfully established.");
					}
				}

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
		finally
		{
			closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to oracle database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		Connection connection = null;

		final String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);

		try
		{
			connection = getConnection(ds);
			metadata = metadataByConnection(connection, schemaName, ds.getId());
		}
		catch ( ClassNotFoundException | SQLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		}
		finally
		{
			closeSqlObject(connection);
		}
		return metadata;
	}

	private Connection getConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}
		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String sid = getConnectionParam(ds, PARAM_SID_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
		final String serviceName = getConnectionParam(ds, PARAM_SERVICE_NAME_ID);

		return getConnection(hostName, portNumber, sid, serviceName, userName, password, schemaName);
	}

	private Connection getConnection(String hostName, String portNumber, String sid, String serviceName, String userName, String password, String schemaName) throws SQLException, ClassNotFoundException, DataExtractionServiceException
	{
		Connection connection = null;
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = getUrl(hostName, portNumber, sid, serviceName);
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private String getUrl(String hostName, String portNumber, String sid, String serviceName) throws DataExtractionServiceException
	{
		String url = null;
		if( (StringUtils.isBlank(sid) && StringUtils.isBlank(serviceName)) || (StringUtils.isNotBlank(sid) && StringUtils.isNotBlank(serviceName)) )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message("Either SID or Service Name need to be entered"));
		}

		if( StringUtils.isNotBlank(sid) )
		{
			url = "jdbc:oracle:thin:@" + hostName + ":" + portNumber + ":" + sid;
		}
		else if( StringUtils.isNotBlank(serviceName) )
		{
			url = "jdbc:oracle:thin:@//" + hostName + ":" + portNumber + "/" + serviceName;
		}
		return url;
	}

	private Metadata metadataByConnection(Connection connection, String schemaName, String dataSourceId) throws DataExtractionServiceException, SQLException
	{

		boolean checkDbStatus = false;
		List<String> schemaList = null;
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		List<String> dataSourceList = getAllDatasourcesFromDatabase(connection);

		if( dataSourceList == null )
		{
			throw new IllegalArgumentException("No Schemas found in datasource '" + dataSourceId + "'");
		}

		if( StringUtils.isNotBlank(schemaName) )
		{
			for (String dbName : dataSourceList)
			{
				if( schemaName.equals(dbName) )
				{
					checkDbStatus = true;
				}
			}
			if( !checkDbStatus )
			{
				throw new IllegalArgumentException("Schema '" + schemaName + "' not found in datasource '" + dataSourceId + "'");
			}
			else
			{
				schemaList = new ArrayList<String>();
				schemaList.add(schemaName);
			}
		}
		else
		{
			schemaList = new ArrayList<String>();
			schemaList.addAll(dataSourceList);
		}

		for (String dataSource : schemaList)
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
				List<Constraint> pkConstraintList = getTableRelatedPkInfo(connection, dataSource, namedType.getName());
				if( pkConstraintList != null )
				{
					for (Constraint constraint : pkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				/* add foreign keys here */
				List<Constraint> fkConstraintList = getTableRelatedFkInfo(connection, dataSource, namedType.getName());
				if( fkConstraintList != null )
				{
					for (Constraint constraint : fkConstraintList)
					{
						pkFkConstraintList.add(constraint);
					}
				}
				namedType.setName(fromSegments(dataSource, namedType.getName()).toString());
				entryType.setConstraints(pkFkConstraintList);
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	private List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException
	{
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<NamedType> tableList = new ArrayList<>();
		try
		{
			String tablesQuery = " select TABLE_NAME from SYS.ALL_TABLES  where OWNER = ? union all select VIEW_NAME from SYS.ALL_VIEWS where OWNER =  ? ";
			preparedStatement = con.prepareStatement(tablesQuery);
			preparedStatement.setString(1, schemaName);
			preparedStatement.setString(2, schemaName);
			resultSet = preparedStatement.executeQuery();
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
			closeSqlObject(resultSet, preparedStatement);
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
			if( con != null )
			{
				columnsQuery = "SELECT column_name, data_type , DATA_LENGTH  FROM all_tab_columns where table_name = ? AND OWNER= ? ";
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
				type.setSize(resultSet.getInt("COLUMN_SIZE"));
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

	private List<Constraint> getTableRelatedFkInfo(Connection con, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		List<Constraint> constraintList = new ArrayList<>();
		try
		{
			if( con != null )
			{
				DatabaseMetaData meta = con.getMetaData();
				resultSet = meta.getImportedKeys(null, schemaName, tableName);
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
							constraint.setTarget(resultSet.getString("FKTABLE_SCHEM") + "." + resultSet.getString("PKTABLE_NAME"));
						}
					}
					constraint.setAttributes(pkOfFkAttributes);
					constraint.setTargetAttributes(fkAttributes);
					constraintList.add(constraint);
				}
			}
		}
		finally
		{
			closeSqlObject(resultSet);
		}

		return constraintList;
	}

	private List<Constraint> getTableRelatedPkInfo(Connection con, String schemaName, String tableName) throws SQLException
	{
		ResultSet resultSet = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try
		{
			if( con != null )
			{
				DatabaseMetaData meta = con.getMetaData();
				// ORACLE_DB_TYPE
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

	private boolean isWantedSchema(String schemaName)
	{
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
		defaultSchemas = new String[] { "ANONYMOUS", "APEX_030200", "APEX_040200", "GSMCATUSER", "GSMUSER", "AUDSYS", "DVF", "DVSYS", "GSMADMIN_INTERNAL", "GSMCATUSER", "LBACSYS", "OJVMSYS", "SYSBACKUP", "SYSDG", "SYSKM", "APEX_PUBLIC_USER", "APPQOSSYS", "BI", "CTXSYS", "DBSNMP", "DIP", "EXFSYS",
				"FLOWS_FILES", "IX", "MDDATA", "MDSYS", "MGMT_VIEW", "OE", "OLAPSYS", "ORACLE_OCM", "ORDDATA", "ORDPLUGINS", "ORDSYS", "OUTLN", "OWBSYS", "OWBSYS_AUDIT", "PM", "SCOTT", "SH", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "SYS", "SYSMAN", "SYSTEM", "WMSYS",
				"XDB", "XS$NULL", "APEX_040000" };
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
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		Connection connection = null;
		try
		{
			String tableName = spec.getCollection();
			connection = getConnection(ds);
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
			final DataExtractionThread dataExtractionExecutor = new OracleDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class OracleDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public OracleDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

				final String dataSourceHost = getConnectionParam(ds, OracleDataSourceAdapter.PARAM_HOST_ID);
				final String dataSourcePort = getConnectionParam(ds, OracleDataSourceAdapter.PARAM_PORT_ID);
				String sid = StringUtils.isNotBlank(getConnectionParam(ds, OracleDataSourceAdapter.PARAM_SID_ID)) ? getConnectionParam(ds, OracleDataSourceAdapter.PARAM_SID_ID) : "";
				final String dataSourceUser = getConnectionParam(ds, OracleDataSourceAdapter.PARAM_USER_ID);
				final String dataSourcePassword = getConnectionParam(ds, OracleDataSourceAdapter.PARAM_PASSWORD_ID);
				String schemaName = getConnectionParam(ds, OracleDataSourceAdapter.PARAM_SCHEMA_ID);
				String serviceName = StringUtils.isNotBlank(getConnectionParam(ds, OracleDataSourceAdapter.PARAM_SERVICE_NAME_ID)) ? getConnectionParam(ds, OracleDataSourceAdapter.PARAM_SERVICE_NAME_ID) : "";

				String tableName = spec.getCollection();
				String[] databaseTable = parse(tableName).segments();

				if( databaseTable.length == 2 )
				{
					schemaName = databaseTable[0].replaceAll(" ", "\\ ");
					tableName = databaseTable[1].replaceAll(" ", "\\ ");
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownCollection").message("Unknown collection '" + tableName + "'"));
				}
				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,

						dataSourcePassword, tableName, getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), dataSourceHost, dataSourcePort, sid, serviceName

						, schemaName, job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

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
		 * @param sid
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

				String dataSourcePort, String sid, String serviceName, String dataSourceName, String jobId, String offset, String limit,

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

			ilParamsVals.put("DATASOURCE_SID", sid);

			ilParamsVals.put("DATASOURCE_SERVICE_NAME", serviceName);

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
