package com.prifender.des.adapter.relational.oracle;

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
public final class OracleDataSourceAdapter extends DataSourceAdapter
{
    @Value( "${des.home}" )
    private String desHome;
    
    private static final String JOB_NAME = "local_project.prifender_oracle_v1_0_1.Prifender_Oracle_v1";

	private static final String DEPENDENCY_JAR = "prifender_oracle_v1_0_1.jar";

	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;
	
    private static final String TYPE_ID = "Oracle";
    public static final String TYPE_LABEL = "Oracle";
    
    public static final String PARAM_SID_ID = "SID";
    public static final String PARAM_SID_LABEL = "SID";
    public static final String PARAM_SID_DESCRIPTION = "The name of the Oracle instance on the specified host";
    
    public static final ConnectionParamDef PARAM_SID 
        = new ConnectionParamDef().id( PARAM_SID_ID ).label( PARAM_SID_LABEL ).description( PARAM_SID_DESCRIPTION ).type( TypeEnum.STRING );

    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID ).label( TYPE_LABEL )
        .addConnectionParamsItem( PARAM_HOST )
        .addConnectionParamsItem( clone( PARAM_PORT ).required( false ).defaultValue( "1521" ) )
        .addConnectionParamsItem( PARAM_SID )
        .addConnectionParamsItem( PARAM_USER )
        .addConnectionParamsItem( PARAM_PASSWORD )
        .addConnectionParamsItem( PARAM_DATABASE );

    @Override
    public DataSourceType getDataSourceType()
    {
        return TYPE;
    }

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;

		try {
			connection = getConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("oracle connection successfully established.");
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

		try {
			connection = getConnection(ds);
			metadata = metadataByConnection(connection, databaseName);
			if (metadata == null) {
				throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return metadata;
	}

	public Connection getConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String Sid = getConnectionParam(ds, PARAM_SID_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		return getDatabaseConnection(hostName, portNumber, Sid, userName, password, databaseName);
	}

	public Connection getDatabaseConnection(String hostName, String portNumber, String Sid, String userName,
			String password, String databaseName) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@" + hostName + ":" + portNumber + ":" + Sid;
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		if (databaseName != null) { 
			connection.setSchema(databaseName);
		}
		return connection;
	}
	public Metadata metadataByConnection(Connection con,String schemaName) throws DataExtractionServiceException, SQLException {
		Metadata metadata = new Metadata();
		List<String> databaseList = null;
		databaseList = getAllDatasourcesFromDatabase(con);
			
			if ( databaseList.size() == 0) {
				throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
			}
			
			if (schemaName != null) {
				if (databaseList.contains(schemaName)) {
					databaseList = new ArrayList<String>();
					databaseList.add(schemaName);
				} else {
					throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
				}
			}
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for(String dataSource : databaseList){
				List<NamedType> tableList =	getDatasourceRelatedTables(con,dataSource);
				for(NamedType namedType : tableList ){
					/* table entry type */
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getTableRelatedColumns(con,dataSource,namedType.getName());
					entryType.setAttributes(attributeListForColumns);
					
					/* added primary keys here */
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList =	getTableRelatedPkInfo(con,dataSource,namedType.getName());
					if(pkConstraintList != null){
						
						 for(Constraint constraint : pkConstraintList){ 
							 pkFkConstraintList.add(constraint);
						 } 
					}
					/* added foreign keys here */
					List<Constraint> fkConstraintList =	getTableRelatedFkInfo(con,dataSource,namedType.getName());
					if(fkConstraintList != null){
						 for(Constraint constraint : fkConstraintList){ 
							 pkFkConstraintList.add(constraint);
						 } 
					}
					namedType.setName(schemaName + "." + namedType.getName());
					entryType.setConstraints(pkFkConstraintList);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
		return metadata;
	}
	public List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<NamedType> tableList = new ArrayList<>();
		try {
					   String tablesQuery = " select TABLE_NAME from SYS.ALL_TABLES  where OWNER = ? union all select VIEW_NAME from SYS.ALL_VIEWS where OWNER =  ? ";
						preparedStatement = con.prepareStatement(tablesQuery);
						preparedStatement.setString(1, schemaName);
						preparedStatement.setString(2, schemaName);
						resultSet = preparedStatement.executeQuery();
						while (resultSet.next()) {
							NamedType namedType = new  NamedType();
							namedType.setName(resultSet.getString(1));
							Type type = new Type().kind(Type.KindEnum.LIST);
							namedType.setType(type);
							tableList.add(namedType);
						
						}
				Collections.sort(tableList, new Comparator<NamedType>() {
					public int compare(NamedType result1, NamedType result2) {
						return result1.getName().compareToIgnoreCase(result2.getName());
					}
				});
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}

		return tableList;
	}

	public List<NamedType> getTableRelatedColumns(Connection con,String schemaName, String tableName) throws SQLException, DataExtractionServiceException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		String columnsQuery = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			if (con != null) {
					columnsQuery = "SELECT column_name, data_type , DATA_LENGTH  FROM all_tab_columns where table_name = ? AND OWNER= ? ";
					preparedStatement = con.prepareStatement(columnsQuery);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, schemaName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						NamedType attributeForColumn = new  NamedType();
						attributeForColumn.setName(resultSet.getString(1));
						Type columnInfo = getColumnInfo(con,schemaName,tableName,resultSet.getString(1));
						if(columnInfo != null)
						{ 
							Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
														   .dataType(getDataType(resultSet.getString(2).toUpperCase())) 
														   .nullable(columnInfo.getNullable())
														   .autoIncrement(columnInfo.getAutoIncrement())
														   .size(columnInfo.getSize());
						    attributeForColumn.setType(typeForCoumn);
							attributeList.add(attributeForColumn);
						 }
					}
				} 
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}
		return attributeList;
	}
	private Type getColumnInfo(Connection connection,String dataBaseName,String tableName,String columnName) throws DataExtractionServiceException{
		Type type= new Type();
		ResultSet resultSet = null;
		try {
			DatabaseMetaData meta = connection.getMetaData();
			resultSet = meta.getColumns(null, dataBaseName, tableName,columnName);
			if (resultSet.next()) {
				type.setSize(resultSet.getInt("COLUMN_SIZE"));
				type.setAutoIncrement(resultSet.getString("IS_AUTOINCREMENT").equals("YES") ? true : false);
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false); 
			} 
		} catch (SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}finally{
			
			 closeSqlObject(resultSet);
		}
		return type;
	}
	public List<Constraint> getTableRelatedFkInfo(Connection con,String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			if (con != null) {
				DatabaseMetaData meta = con.getMetaData();
					resultSet = meta.getExportedKeys(null, schemaName, tableName);
					while (resultSet.next()) { 
						 boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
						 List<String> fkAttributes = new ArrayList<>();
						 List<String> pkOfFkAttributes = new ArrayList<>();
						  Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);;
						  if(StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME"))){
							  if(isForeignKey){
								  fkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
								  pkOfFkAttributes.add(resultSet.getString("PKCOLUMN_NAME"));
								  constraint.setTarget(resultSet.getString("FKTABLE_SCHEM")+"."+resultSet.getString("FKTABLE_NAME"));
							  }
						  }
						  constraint.setAttributes(pkOfFkAttributes);
						  constraint.setTargetAttributes(fkAttributes);
						  constraintList.add(constraint);
					}
			}
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}

		return constraintList;
	}
	public List<Constraint> getTableRelatedPkInfo(Connection con,String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparesStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			if (con != null) {
				   DatabaseMetaData meta = con.getMetaData();
					resultSet = meta.getPrimaryKeys(schemaName, null, tableName);
						 List<String> pkAttributes = new ArrayList<>();
						 Constraint constraint = new Constraint();
						while (resultSet.next()) {
							boolean isPrimarykey = resultSet.getString("COLUMN_NAME") != null ? true : false;
							  if(StringUtils.isNotBlank(resultSet.getString("COLUMN_NAME"))){
								  if(isPrimarykey){
									  pkAttributes.add(resultSet.getString("COLUMN_NAME"));
								  }
							  }
							}
						if(pkAttributes != null && pkAttributes.size() > 0){
							 constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
						 }
						 constraint.setAttributes(pkAttributes);
						 constraintList.add(constraint);
					}
		} finally {
			closeSqlObject(resultSet, preparesStatement);
		}
		return constraintList;
	}
	public List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		try {
			if (con != null) {
				DatabaseMetaData meta = con.getMetaData();
				resultSet = meta.getSchemas();
				while (resultSet.next()) {
					String schema = resultSet.getString("TABLE_SCHEM");
					if (isWantedSchema(schema)) {
						schemaList.add(schema);
					}
				  }
				}
				Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}
		return schemaList;
	}
	  
	public boolean isWantedSchema(String schemaName) {
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
			defaultSchemas = new String[] { "ANONYMOUS"/*, "APEX_030200", "APEX_PUBLIC_USER", "APPQOSSYS", "BI", "CTXSYS",
					"DBSNMP", "DIP", "EXFSYS", "FLOWS_FILES", "HR", "IX", "MDDATA", "MDSYS", "MGMT_VIEW", "OE",
					"OLAPSYS", "ORACLE_OCM", "ORDDATA", "ORDPLUGINS", "ORDSYS", "OUTLN", "OWBSYS", "OWBSYS_AUDIT", "PM",
					"SCOTT", "SH", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "SYS",
					"SYSMAN", "SYSTEM", "WMSYS", "XDB", "XS$NULL"*/ }; 
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
			connection = getConnection(ds);
			if (connection != null) {
				countRows =  getCountRows(connection, tableName);
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return countRows;
	}
	 
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec ,int containersCount) throws DataExtractionServiceException 
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
					
					.id(spec.getDataSource() + "-" + tableName + "-" + getUUID())
					
					.state(DataExtractionJob.StateEnum.WAITING);
				 
 
				String adapterHome =  createDir(this.desHome, TYPE_ID);
				
				startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
		} 
		catch (Exception err) 
		{
			throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
		}
		
		return startResult;
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
			
			DataExtractionJob job, int rowCount, String adapterHome, int containersCount)
	
			throws DataExtractionServiceException {
		
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
				
				int taskSampleSize =  generateTaskSampleSize( totalSampleSize , containersCount );
				
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
	 * @param ds
	 * @param spec
	 * @param job
	 * @param adapterHome
	 * @param offset
	 * @param limit
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec ,
			
			DataExtractionJob job,String adapterHome,int offset,int limit) throws DataExtractionServiceException
	{
		
		DataExtractionTask dataExtractionTask = new DataExtractionTask();
		 
		try
		{
		 
		final String dataSourceHost = getConnectionParam( ds , PARAM_HOST_ID );
		
		final String dataSourcePort = getConnectionParam( ds , PARAM_PORT_ID);
		
		final String dataSourceUser = getConnectionParam( ds , PARAM_USER_ID );
		
		final String dataSourcePassword = getConnectionParam( ds , PARAM_PASSWORD_ID );
		
		String databaseName = getConnectionParam( ds , PARAM_DATABASE_ID );
		
		final String sid = getConnectionParam(ds, PARAM_SID_ID);
		
		String tableName = spec.getCollection( );
		
		String[] databaseTable = StringUtils.split( tableName , "." );
		
		if ( databaseTable.length == 2 )
		{
			
			databaseName = databaseTable[0];
			
			tableName = databaseTable[1];
			
		}
		
		 
		Map< String , String > contextParams = getContextParams( adapterHome , JOB_NAME , dataSourceUser ,
				
				dataSourcePassword , tableName , getDataSourceColumnNames(ds, spec,".") , dataSourceHost , dataSourcePort ,sid,
				
				databaseName , job.getOutputMessagingQueue() , String.valueOf(offset) , String.valueOf( limit )  ,
				
				String.valueOf( DataExtractionSpec.ScopeEnum.SAMPLE ) , String.valueOf( limit ) );
		
		dataExtractionTask.taskId("DES-Task-"+ getUUID( ))
						
						                    .jobId( job.getId( ) )
				                           
				                            .typeId( TYPE_ID +"__"+JOB_NAME + "__" +DEPENDENCY_JAR )
				                           
				                            .contextParameters( contextParams )
				                           
				                            .numberOfFailedAttempts( 0 );
		}
		catch(Exception e)
		{
			throw new DataExtractionServiceException( new Problem( ).code( "Error" ).message( e.getMessage() ) );
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

			String dataSourcePort, String sid, String dataSourceName, String jobId, String offset, String limit,

			String dataSourceScope, String dataSourceSampleSize) throws IOException {

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME",  getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);
		
		ilParamsVals.put("JOB_NAME", jobName);
		
		ilParamsVals.put("DATASOURCE_USER", dataSourceUser);
		
		ilParamsVals.put("DATASOURCE_PASS", dataSourcePassword);
		
		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);
		
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		
		ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);
		
		ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);
		
		ilParamsVals.put("DATASOURCE_SID", sid);
		
		ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
		
		ilParamsVals.put("JOB_ID", jobId);
		
		ilParamsVals.put("OFFSET", offset);
		
		ilParamsVals.put("LIMIT", limit);
		
		ilParamsVals.put("SCOPE", dataSourceScope);
		
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		return ilParamsVals;

	}	

}
