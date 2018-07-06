package com.prifender.des.adapter.relational.mysql;

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
import java.sql.Statement;
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
public final class MySqlDataSourceAdapter extends DataSourceAdapter {

	@Value( "${des.home}")
	private String desHome;

	private static final String JOB_NAME = "local_project.prifender_mysql_v1_0_1.Prifender_MYSQL_v1";

	private static final String DEPENDENCY_JAR = "prifender_mysql_v1_0_1.jar";

	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;	

	public static final String TYPE_ID = "MySql";
    public static final String TYPE_LABEL = "MySQL";
    
   
    
    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID ).label( TYPE_LABEL )
        .addConnectionParamsItem( PARAM_HOST )
        .addConnectionParamsItem( clone( PARAM_PORT ).required( false ).defaultValue( "3306" ) )
        .addConnectionParamsItem( PARAM_USER )
        .addConnectionParamsItem( PARAM_PASSWORD )
        .addConnectionParamsItem( PARAM_DATABASE );

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;

		try {
			connection = getConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Mysql connection successfully established.");
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
			if (connection != null) {
				metadata = metadataByConnection(connection, databaseName);
				if (metadata == null) {
					throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return metadata;
	}

	private int getTableCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getConnection(ds);
			countRows =  getCountRows(connection, tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return countRows;
	}


	private Connection getConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
		return getDataBaseConnection(hostName, portNumber, userName, password, databaseName);
	}

	private Connection getDataBaseConnection(String hostName, String portNumber, String userName, String password,
			String databaseName) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://" + hostName + ":" + portNumber + ( databaseName != null ? "/" + databaseName : "");
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection con, String dataSourceName)
			throws SQLException, DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		
		dataSourceList = getAllDatasourcesFromDatabase(con);
		
		if ( dataSourceList.size() == 0) {
			throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
		}
		if (dataSourceName != null) { 
			if( dataSourceList.size() > 0 && dataSourceList.contains(dataSourceName) ) {
				dataSourceList = new ArrayList<>();
				dataSourceList.add(dataSourceName);
			} else {
				throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
			}
		}	
				
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (String dataSource : dataSourceList) {
			List<NamedType> tableList = getDatasourceRelatedTables(con, dataSource);
			for (NamedType namedType : tableList) {
				// table entry type
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getTableRelatedColumns(con, dataSource, namedType.getName());
				entryType.setAttributes(attributeListForColumns);

				// add primary keys here
				List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
				List<Constraint> pkConstraintList = getTableRelatedPrimarykeyInfo(con, dataSource, namedType.getName());
				if (pkConstraintList != null) {
					for (Constraint constraint : pkConstraintList) {
						pkFkConstraintList.add(constraint);
					}
				}
				// add foreign keys here
				List<Constraint> fkConstraintList = getTableRelatedForeignkeyInfo(con, dataSource, namedType.getName());
				if (fkConstraintList != null) {
					for (Constraint constraint : fkConstraintList) {
						pkFkConstraintList.add(constraint);
					}
				}
				entryType.setConstraints(pkFkConstraintList); 
				namedType.setName(dataSource + "." + namedType.getName());
				namedTypeObjectsList.add(namedType);
				break;
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	private List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException {
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;
		try {
			 String[] types = { "TABLE" };
			resultSet= con.getMetaData()
			         .getTables(schemaName, null,"%", types);
			while (resultSet.next()) {
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString(3));
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
			closeSqlObject(resultSet);
		}

		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(Connection con, String schemaName, String tableName)
			throws SQLException, DataExtractionServiceException {
		ResultSet resultSet = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			resultSet= con.getMetaData()
			         .getColumns(schemaName,null, tableName,"%");
			while (resultSet.next()) {
				String columnName = resultSet.getString("COLUMN_NAME");
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(columnName);
			 	Type columnInfo = getColumnInfo(con,schemaName,tableName,columnName);
				if(columnInfo != null)
				{ 
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
												   .dataType(getDataType(columnName.toUpperCase())) 
												   .nullable(columnInfo.getNullable())
												   .autoIncrement(columnInfo.getAutoIncrement())
												   .size(columnInfo.getSize());
				    attributeForColumn.setType(typeForCoumn);
					attributeList.add(attributeForColumn);
				 }
			}
		} finally {
			closeSqlObject(resultSet);
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
				type.setSize((resultSet.getInt("COLUMN_SIZE")));
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
	public List<Constraint> getTableRelatedForeignkeyInfo(Connection con, String schemaName, String tableName)
			throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getExportedKeys(schemaName, schemaName, tableName);
			while (resultSet.next()) {
				boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
				List<String> fkAttributes = new ArrayList<>();
				List<String> pkOfFkAttributes = new ArrayList<>();
				Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
				if (StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME"))) {
					if (isForeignKey) {
						fkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
						pkOfFkAttributes.add(resultSet.getString("PKCOLUMN_NAME"));
						constraint.setTarget(resultSet.getString("FKTABLE_CAT")+"."+resultSet.getString("FKTABLE_NAME"));
					}
				}
				constraint.setAttributes(pkOfFkAttributes);
				constraint.setTargetAttributes(fkAttributes);
				constraintList.add(constraint);
			}
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}

		return constraintList;
	}

	private List<Constraint> getTableRelatedPrimarykeyInfo(Connection con, String schemaName, String tableName)
			throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getPrimaryKeys(schemaName, null, tableName);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (resultSet.next()) {
				boolean isPrimarykey = resultSet.getString("COLUMN_NAME") != null ? true : false;
				if (StringUtils.isNotBlank(resultSet.getString("COLUMN_NAME"))) {
					if (isPrimarykey) {
						pkAttributes.add(resultSet.getString("COLUMN_NAME"));
					}
				}
			}
			if (pkAttributes != null && pkAttributes.size() > 0) {
				constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
			}
			constraint.setAttributes(pkAttributes);
			constraintList.add(constraint);
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}
		return constraintList;
	}

	private List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		Statement statement = null;
		try {
			if (con != null) {
				resultSet= con.getMetaData().getCatalogs();
				while (resultSet.next()) {
					schemaList.add(resultSet.getString(1));
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);
		} finally {
			closeSqlObject(resultSet, statement);
		}
		return schemaList;
	}
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final int containersCount)
			throws DataExtractionServiceException {
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
 
			String adapterHome =  createDir(this.desHome, TYPE_ID);
				
			startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
		}
 		catch (Exception exe) 
		{
			throw new DataExtractionServiceException(new Problem().code("job error").message(exe.getMessage()));
			
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
		
		final String dataSourcePort = getConnectionParam( ds , PARAM_PORT_ID );
		
		final String dataSourceUser = getConnectionParam( ds , PARAM_USER_ID );
		
		final String dataSourcePassword = getConnectionParam( ds , PARAM_PASSWORD_ID );
		
		String databaseName = getConnectionParam( ds , PARAM_DATABASE_ID );
		
		String tableName = spec.getCollection( );
		
		String[] databaseTable = StringUtils.split( tableName , "." );
		
		if ( databaseTable.length == 2 )
		{
			
			databaseName = databaseTable[0];
			
			tableName = databaseTable[1];
			
		}
 
		Map< String , String > contextParams = getContextParams( adapterHome , JOB_NAME , dataSourceUser ,
				
				dataSourcePassword , tableName , getDataSourceColumnNames(ds, spec,".") , dataSourceHost , dataSourcePort ,
				
				databaseName , job.getOutputMessagingQueue() , String.valueOf(offset) , String.valueOf( limit )  ,
				
				String.valueOf( DataExtractionSpec.ScopeEnum.SAMPLE ) , String.valueOf( limit ) );
		
		dataExtractionTask.taskId("DES-Task-"+getUUID( ))
		
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

	public Map< String , String > getContextParams(String jobFilesPath, String jobName, String dataSourceUser,
			
			String dataSourcePassword, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,
			
			String dataSourcePort, String dataSourceName, String jobId, String offset, String limit,
			
			String dataSourceScope, String dataSourceSampleSize) throws IOException
	{

		Map< String , String > ilParamsVals = new LinkedHashMap<>( );
		
		ilParamsVals.put( "JOB_STARTDATETIME" ,  getConvertedDate( new Date( ) ) );
		
		ilParamsVals.put( "FILE_PATH" , jobFilesPath );
		
		ilParamsVals.put( "JOB_NAME" , jobName );
		
		ilParamsVals.put( "DATASOURCE_USER" , dataSourceUser );
		
		ilParamsVals.put( "DATASOURCE_PASS" , dataSourcePassword );
		
		ilParamsVals.put( "DATASOURCE_TABLE_NAME" , dataSourceTableName );
		
		ilParamsVals.put( "DATASOURCE_COLUMN_NAMES" , dataSourceColumnNames );
		
		ilParamsVals.put( "DATASOURCE_HOST" , dataSourceHost );
		
		ilParamsVals.put( "DATASOURCE_PORT" , dataSourcePort );
		
		ilParamsVals.put( "DATASOURCE_NAME" , dataSourceName );
		
		ilParamsVals.put( "JOB_ID" , jobId );
		
		ilParamsVals.put( "OFFSET" , offset );
		
		ilParamsVals.put( "LIMIT" , limit );
		
		ilParamsVals.put( "SCOPE" , dataSourceScope );
		
		ilParamsVals.put( "SAMPLESIZE" , dataSourceSampleSize );
		
		return ilParamsVals;

	}
}
