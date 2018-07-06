package com.prifender.des.adapter.relational.sqlserver;

import static com.prifender.des.util.DatabaseUtil.closeSqlObject;

import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getCountRows;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
//import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
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

@Component
public final class SqlServerDataSourceAdapter extends DataSourceAdapter {

    @Value( "${des.home}" )
    private String desHome;
    
    private static final String JOB_NAME = "local_project.prifender_mssql_v1_0_1.Prifender_MSSQL_v1";
	
	private static final String DEPENDENCY_JAR = "prifender_mssql_v1_0_1.jar";
	
	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;

    public static final String TYPE_ID = "SqlServer";
    
    public static final String TYPE_LABEL = "Microsoft SQL Server";
    
    // Instance

    public static final String PARAM_INSTANCE_ID = "Instance";
    public static final String PARAM_INSTANCE_LABEL = "Instance";
    public static final String PARAM_INSTANCE_DESCRIPTION = "The name of the instance";
    
    public static final ConnectionParamDef PARAM_INSTANCE
        = new ConnectionParamDef().id( PARAM_INSTANCE_ID ).label( PARAM_INSTANCE_LABEL ).description( PARAM_INSTANCE_DESCRIPTION ).type( TypeEnum.STRING ).required( false );
    
	private static final DataSourceType TYPE = new DataSourceType()
	    .id( TYPE_ID ).label( TYPE_LABEL )
        .addConnectionParamsItem( PARAM_HOST )
        .addConnectionParamsItem( clone( PARAM_PORT ).required( false ).defaultValue( "1433" ) )
        .addConnectionParamsItem( PARAM_USER )
        .addConnectionParamsItem( PARAM_PASSWORD )
        .addConnectionParamsItem( PARAM_INSTANCE )
        .addConnectionParamsItem( PARAM_DATABASE )
        .addConnectionParamsItem( PARAM_SCHEMA );

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS)
						.message("SqlServer connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to oracle database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		final String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
		try {
			connection = getDataBaseConnection(ds);
			metadata = metadataByConnection(connection, schemaName, databaseName);
			if (metadata == null) {
				throw new DataExtractionServiceException(
						new Problem().code("metadata error").message("meta data not found for connection."));
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return metadata;
	}

	private Connection getDataBaseConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String port = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
        final String instance = getConnectionParam(ds, PARAM_INSTANCE_ID);
        return getDataBaseConnection(hostName, port, databaseName, userName, password,instance);
	}

	private Connection getDataBaseConnection(String hostName, String port, String databaseName, String userName,
			String password,String instance) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://" + hostName + ":" + port + ";" + ( databaseName != null ? "databaseName=" + databaseName : "" )+";"+( instance != null ? "instance=" + instance : "" );
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection con, String schemaName, String databaseName) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		try {
			dataSourceList = getAllDatasourcesFromDatabase(con);
			if ( dataSourceList.size() == 0) {
				throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
			}
			if (dataSourceList.size() > 0) {
				if (schemaName != null) {
					if (dataSourceList.contains(schemaName)) {
						dataSourceList = new ArrayList<String>();
						dataSourceList.add(schemaName);
					} else {
						throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
					}
				}
			}  
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String dataSource : dataSourceList) {
				List<NamedType> tableList = getSchemaRelatedTables(con, schemaName,databaseName);
				for (NamedType namedType : tableList) {
					/* table entry type */
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);

					String tableName = namedType.getName().split("\\.")[2] ;
					List<NamedType> attributeListForColumns = getTableRelatedColumns(con, databaseName,schemaName, tableName);
					entryType.setAttributes(attributeListForColumns);

					/* add primary keys here */
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList = getTableRelatedPkInfo(con,databaseName, dataSource, tableName);
					if (pkConstraintList != null) {
						for (Constraint constraint : pkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}
					/* add foreign keys here */
					List<Constraint> fkConstraintList = getTableRelatedFkInfo(con,databaseName, dataSource, namedType.getName());
					if (fkConstraintList != null) {
						for (Constraint constraint : fkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}
					entryType.setConstraints(pkFkConstraintList);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (DataExtractionServiceException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}
		return metadata;
	}

	private List<NamedType> getSchemaRelatedTables(Connection con, String schemaName,String databaseName) throws SQLException {
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;
		try {
			if (con != null) {
				DatabaseMetaData databaseMetadata = con.getMetaData();
					 resultSet = databaseMetadata.getTables(databaseName, schemaName, "%",  new String[] {"TABLE"});
					while (resultSet.next()) {
						String tableName = resultSet.getString(3);
						NamedType namedType = new NamedType();
						namedType.setName(databaseName+"."+schemaName+"."+tableName);
						Type type = new Type().kind(Type.KindEnum.LIST);
						namedType.setType(type);
						tableList.add(namedType);

					}
				}
				Collections.sort(tableList, new Comparator<NamedType>() {
					public int compare(NamedType result1, NamedType result2) {
						return result1.getName().compareToIgnoreCase(result2.getName());
					}
				});
		}
		 finally {
			closeSqlObject(resultSet);
		}

		return tableList;
	}
	
	private List<NamedType> getTableRelatedColumns(Connection con,String databaseName, String schemaName, String tableName) throws SQLException
	{
		ResultSet rs = null;
		DatabaseMetaData meta=null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{
			if( con != null )
			{
				 meta = con.getMetaData();
				 rs = meta.getColumns(databaseName, schemaName, tableName,null);
				 while (rs.next())
					{
					 	String columnName = rs.getString("COLUMN_NAME");
						NamedType attributeForColumn = new NamedType();
						attributeForColumn.setName(columnName);
						Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(columnName.toUpperCase()));
						attributeForColumn.setType(typeForCoumn);
						attributeList.add(attributeForColumn);
					}
			}
		}
		finally
		{
			closeSqlObject(rs);
		}
		return attributeList;
	}
	
	
	private List<Constraint> getTableRelatedFkInfo(Connection con,String databaseName, String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		DatabaseMetaData meta=null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			if (con != null) {
				 meta = con.getMetaData( );
				resultSet = meta.getExportedKeys(databaseName, schemaName, tableName);
				while (resultSet.next()) {
					if (resultSet.getString("FKCOLUMN_NAME") != null) {
						boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
						List<String> pkOfFkAttributes = new ArrayList<>();
						Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
						
						if (StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME"))) {
							if (isForeignKey) {
								
								pkOfFkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
								constraint.setTarget(resultSet.getString("FKTABLE_CAT")+"."+resultSet.getString("FKTABLE_SCHEM")+"."+resultSet.getString("FKCOLUMN_NAME"));
							}
						}
						constraint.setAttributes(pkOfFkAttributes);
						constraintList.add(constraint);
					}
				}
			}
		} finally {
			closeSqlObject(resultSet);
		}
		return constraintList;
	}

	private List<Constraint> getTableRelatedPkInfo(Connection con, String dataBaseName,String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		DatabaseMetaData dm=null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			if (con != null) {
				 dm = con.getMetaData( );
			    resultSet = dm.getPrimaryKeys(dataBaseName, schemaName,tableName );
		
				List<String> pkAttributes = new ArrayList<>();
				Constraint constraint = new Constraint();
				while (resultSet.next()) {
						pkAttributes.add(resultSet.getString("COLUMN_NAME"));
				}
				if (pkAttributes != null && pkAttributes.size() > 0) {
					constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
					constraint.setAttributes(pkAttributes);
					constraintList.add(constraint);
				}
			}
		} finally {
			closeSqlObject(resultSet);
		}
		return constraintList;
	}

	private List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		List<String> dataBaseList = new ArrayList<>();
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData=null;
		try {
			if (con != null) {
				databaseMetaData = con.getMetaData();
				resultSet = databaseMetaData.getCatalogs();
				while (resultSet.next()) {
					String dataBase = resultSet.getString("TABLE_CAT");
					if (isWantedDataBase(dataBase)) {
						dataBaseList.add(dataBase);
					}
				}
				List<String> schemaNameList = getSchemaByDatabse(con,dataBaseList.toString());
				for (String schema : schemaNameList) {
					schemaList.add(schema);	
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);
			}
		} finally {
			closeSqlObject(resultSet);
		}
		return schemaList;
	}

	public boolean isWantedDataBase(String schemaName) {
		String[] defaultDataBases = {};
		List<String> defaultDataBaseList = new ArrayList<>();
		defaultDataBases = new String[] { "AdventureWorks2016_EXT" , "chandan", "ContosoRetailDW", "customers_m001v1", "employees_5m", "employees_k010v2",
					"employees_k025v2", "employees_k025v3", "employees_k025v3_copy", "employees_k025v3_partial", "employees_k025v3_truncated", "employees_k050v2", "employees_k100v2", "employees_k100x100v3", "employees_k500v2", "employees_m001",
					"employees_m001v2", "employees_m005v2", "employees_m010", "employees_m010v2", "employees_m050", "employees_m050v2", "kosta_test", "master", "model",
					"msdb", "people_10k", "perf_test", "SELECT", "Table100", "tempdb",
					"WideWorldImporters" }; 
		boolean wantedDataBase = true;
		for (int i = 0; i < defaultDataBases.length; i++) {
			defaultDataBaseList.add(defaultDataBases[i]);
		}
		if (defaultDataBaseList.contains(schemaName)) {
			wantedDataBase = false;
		}
		return wantedDataBase;
	}
	
	
	private List<String> getSchemaByDatabse(Connection con, String databaseName) throws SQLException {
		List<String> schemas = new ArrayList<>();
		ResultSet resultSet = null;
		try {
			resultSet= con.getMetaData().getSchemas();
			while (resultSet.next()) {
				String schema = resultSet.getString("TABLE_SCHEM");
				if (isWantedSchema(schema)) {
					schemas.add(schema);
				}
			}
		} finally {
			closeSqlObject(resultSet);
		}
		return schemas;
	}

	public boolean isWantedSchema(String schemaName) {
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
			defaultSchemas = new String[] { "db_accessadmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_ddladmin", "db_denydatareader",
					"db_denydatawriter", "db_owner", "db_securityadmin", "guest", "INFORMATION_SCHEMA", "sys", "hr"}; 
		boolean wantedSchema = true;
		for (int i = 0; i < defaultSchemas.length; i++) {
			defaultSchemasList.add(defaultSchemas[i]);
		}
		if (defaultSchemasList.contains(schemaName)) {
			wantedSchema = false;
		}
		return wantedSchema;
	}
	
	private int getTableCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			
			String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
			String[] schemaTable = StringUtils.split(tableName, ".");
			if (schemaTable.length == 3 ) {
				schemaName = schemaTable[1];
				tableName = schemaTable[2];
			}
			if (schemaTable.length == 2 ) {
				schemaName = schemaTable[0];
				tableName = schemaTable[1];
			}
			countRows = getCountRows(connection, schemaName + "." + tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return countRows;
	}

	@Override
	public StartResult startDataExtractionJob(final DataSource ds, final DataExtractionSpec spec ,final  int containersCount) throws DataExtractionServiceException 
	{
		StartResult startResult = null;
		try {
			
			String tableName = spec.getCollection();
			
			int rowCount = getTableCountRows(ds, tableName);

			if (rowCount == 0) 
			{
				throw new DataExtractionServiceException( new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			
			String[] schemaTableName = StringUtils.split(tableName, ".");
			
			tableName = schemaTableName[schemaTableName.length - 1];
			
			DataExtractionJob job = new DataExtractionJob()
					
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					
					.state(DataExtractionJob.StateEnum.WAITING);
					
 
			String adapterHome = createDir(this.desHome, TYPE_ID);
				
		    startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
		} 
		catch (Exception exe) 
		{
			
			throw new DataExtractionServiceException(new Problem().code("job error").message(exe.getMessage()));
		}
		
		return startResult;
	}

	/**
	 * Runs the DataExtraction Job
	 * 
	 * @param ds
	 * @param spec
	 * @param job
	 * @param rowCount
	 * @param adapterHome
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec ,
			DataExtractionJob job,int rowCount ,String adapterHome , int containersCount) throws DataExtractionServiceException{
		
		int totalSampleSize = 0;
		
		int tasksCount = 0;
		
		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();
		
		try
		{
				
			if ( spec.getScope( ).equals( DataExtractionSpec.ScopeEnum.SAMPLE ) )
			{
				
				if ( spec.getSampleSize( ) == null )
				{
					
					throw new DataExtractionServiceException( new Problem( ).code( "Meta data error" ).message( "sampleSize value not found" ) );
					
				}
				
				totalSampleSize = rowCount < spec.getSampleSize( ) ? rowCount : spec.getSampleSize( );
				
			}else
			{
				totalSampleSize = rowCount;
			}
			
			   synchronized (job) {
	        	   
	        	   job.setOutputMessagingQueue("DES-" + job.getId());
	        	   
	        	   job.objectsExtracted(0);
			
			   }
			if (totalSampleSize <= MIN_THRESHOULD_ROWS ) {
				
				int offset = 1;
				
				dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  totalSampleSize ));
				
				tasksCount++;
				
			} else {
				
				int taskSampleSize = generateTaskSampleSize( totalSampleSize , containersCount );
				
				if ( taskSampleSize <= MIN_THRESHOULD_ROWS ) 
				{  						 
					taskSampleSize = MIN_THRESHOULD_ROWS ;						 
				}
				if ( taskSampleSize > MAX_THRESHOULD_ROWS ) 
				{  						 
					taskSampleSize = MAX_THRESHOULD_ROWS ;						 
				}
				
				int noOfTasks = totalSampleSize / taskSampleSize ;			
				
				int remainingSampleSize = totalSampleSize % taskSampleSize;		
				
				for (int i = 0 ; i < noOfTasks ; i++) 
				{
					
					int offset = taskSampleSize * i + 1;	
					
					dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  taskSampleSize ));
					
					tasksCount++;
				}
				
				if (remainingSampleSize > 0) 
				{								 
					int offset = noOfTasks * taskSampleSize + 1;
					
					dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  remainingSampleSize ));
					
					tasksCount++;
				}
			}
			 
           synchronized (job) {
         
        	   job.setTasksCount(tasksCount);
        	   
        	   job.setObjectCount(totalSampleSize);
           }

		} catch ( Exception e )
		{
			throw new DataExtractionServiceException( new Problem( ).code( "Error" ).message( e.getMessage() ) ); 
		}
		return dataExtractionJobTasks;
	}

	/**
	 * Gets the connection params which are required for the Data Extraction
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
			DataExtractionJob job, String adapterHome, int offset, int limit) throws DataExtractionServiceException {

		DataExtractionTask dataExtractionTask = new DataExtractionTask();

		try {
			 
			final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID); 
			
			final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);
			
			final String dataSourceUser =  getConnectionParam(ds, PARAM_USER_ID);
			
			final String dataSourcePassword =  getConnectionParam(ds, PARAM_PASSWORD_ID);
			
			String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
			
			String schemaName = getConnectionParam(ds, PARAM_SCHEMA_ID);
			
			final String instanceName = getConnectionParam(ds, PARAM_INSTANCE_ID);
			
			String tableName = spec.getCollection();
			
			String[] schemaTable = StringUtils.split(tableName, ".");
			
			if (schemaTable.length == 3 ) 
			{
				databaseName = schemaTable[0];
				schemaName = schemaTable[1];
				tableName = schemaTable[2];
				
			}
			if (schemaTable.length == 2 ) 
			{
				schemaName = schemaTable[0];
				tableName = schemaTable[1];
			}
			if (databaseName == null) {
				throw new DataExtractionServiceException(new Problem().code("unknown database").message("database name not found in collection"));
			}

			final String dataSourceTableName = schemaName + "." + tableName;
			 
			Map< String , String > contextParams = getContextParams( adapterHome , JOB_NAME , dataSourceUser ,
					
					dataSourcePassword , dataSourceTableName , getDataSourceColumnNames(ds, spec,".") , dataSourceHost , dataSourcePort ,
					
					databaseName ,instanceName, job.getOutputMessagingQueue() , String.valueOf(offset) , String.valueOf( limit )  ,
					
					String.valueOf( DataExtractionSpec.ScopeEnum.SAMPLE ) , String.valueOf( limit ) );
			
			dataExtractionTask.taskId("DES-Task-"+getUUID( ))
							
							                    .jobId( job.getId( ) )
					                           
					                            .typeId( TYPE_ID +"__"+JOB_NAME + "__" +DEPENDENCY_JAR )
					                           
					                            .contextParameters( contextParams )
					                           
					                            .numberOfFailedAttempts( 0 );
			

		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}
		return dataExtractionTask;
	}

	/**
	 *  The Context params for the data extraction process
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
	 * @return Map< String , String >
	 * @throws IOException
	 */
	private Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceUser,
			String dataSourcePassword, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,
			String dataSourcePort, String dataSourceName, String instanceName, String jobId, String offset,
			String limit, String dataSourceScope, String dataSourceSampleSize) throws IOException {
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
		
		ilParamsVals.put("INSTANCE_NAME", instanceName);
		
		ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
		
		ilParamsVals.put("JOB_ID", jobId);
		
		ilParamsVals.put("OFFSET", offset);
		
		ilParamsVals.put("LIMIT", limit);
		
		ilParamsVals.put("SCOPE", dataSourceScope);
		
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		return ilParamsVals;

	}

}