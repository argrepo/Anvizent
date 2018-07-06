package com.prifender.des.adapter.relational.hive;

import static com.prifender.des.util.DatabaseUtil.closeSqlObject;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getCountRows;
import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
public final class HiveDataSourceAdapter extends DataSourceAdapter {

	@Value( "${des.home}" )
	private String desHome;
	 
	private static final String JOB_NAME = "local_project.prifender_hive_v1_0_1.Prifender_Hive_v1";
	
	private static final String DEPENDENCY_JAR = "prifender_hive_v1_0_1.jar";
	
	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;		
	
	private static final String TYPE_ID = "Hive";
	
	private static final String TYPE_LABEL = "Apache Hive";
	
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL)
			.addConnectionParamsItem(PARAM_HOST)
			.addConnectionParamsItem(PARAM_PORT)
			.addConnectionParamsItem(PARAM_USER)
			.addConnectionParamsItem(PARAM_PASSWORD)
			.addConnectionParamsItem(PARAM_DATABASE);

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
						.message("Hive  connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Hive   database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Connection connection = null;
 
		final String databaseName = getConnectionParam(ds, PARAM_DATABASE_ID);
 
		try {
			connection = getDataBaseConnection(ds);
			metadata = metadataByConnection(connection, databaseName);
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
		return getDataBaseConnection(hostName, port, databaseName, userName, password); 
	}

	private Connection getDataBaseConnection(String hostName, String port, String databaseName, String userName,
			String password) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "org.apache.hive.jdbc.HiveDriver";
		Class.forName(driver);
		String url = "jdbc:hive2://" +hostName + ":" + port + "/" +databaseName;
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	private Metadata metadataByConnection(Connection con, String databaseName) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> dataSourceList = new ArrayList<>();
		String dataSourceName = null;
		if (databaseName != null) {
			dataSourceName =  databaseName ;
		}
		try {
			dataSourceList.add(databaseName);
			if(dataSourceList.size()<=0){
			dataSourceList = getAllDatasourcesFromDatabase(con);
			}
			if ( dataSourceList.size() == 0) {
				throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
			}
			if (dataSourceList.size() > 0) {
				if (dataSourceName != null) {
					if (dataSourceList.contains(dataSourceName)) {
						dataSourceList = new ArrayList<String>();
						dataSourceList.add(dataSourceName);
					} else {
						throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
					}
				}
			} 
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String dataSource : dataSourceList) {
				List<NamedType> tableList = getSchemaRelatedTables(con, dataSource);
				for (NamedType namedType : tableList) {
					// table entry type
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);

					List<NamedType> attributeListForColumns = getTableRelatedColumns(con, dataSource, namedType.getName());
					entryType.setAttributes(attributeListForColumns);

					// added primary keys here
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList = getTableRelatedPkInfo(con, dataSource, namedType.getName());
					if (pkConstraintList != null && pkConstraintList.size() > 0) {
						for (Constraint constraint : pkConstraintList) {
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

	private List<Constraint> getTableRelatedPkInfo(Connection con, String dataSource, String tableName) {
		DatabaseMetaData databaseMetaData;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			databaseMetaData = con.getMetaData();
			ResultSet primaryKeysResultSet = databaseMetaData.getPrimaryKeys(null, dataSource, tableName.split("\\.")[1]);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (primaryKeysResultSet.next()) {
					pkAttributes.add(primaryKeysResultSet.getString("PK_NAME") );
			}
			if (pkAttributes != null && pkAttributes.size() > 0) {
				constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
				constraint.setAttributes(pkAttributes);
				constraintList.add(constraint);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		return constraintList;
	}

	@SuppressWarnings("unused")
	private List<Constraint> getTableRelatedFkInfo(Connection con, String dataSource, String tableName) {
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			DatabaseMetaData databaseMetaData = con.getMetaData();
			ResultSet primaryKeysResultSet = databaseMetaData.getImportedKeys(null, null, tableName.split("\\.")[1]);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			System.out.println(tableName+","+ primaryKeysResultSet.next());
			while (primaryKeysResultSet.next()) {
					pkAttributes.add(primaryKeysResultSet.getString("referenced_column_name") );
			}
			if (pkAttributes != null && pkAttributes.size() > 0) {
				constraint.kind(Constraint.KindEnum.FOREIGN_KEY);
				constraint.setAttributes(pkAttributes);
				constraintList.add(constraint);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		return constraintList;
	}
	private List<NamedType> getSchemaRelatedTables(Connection con, String dataSource) {
		DatabaseMetaData databaseMetaData;
		List<NamedType> tableList = new ArrayList<>();
		try {
			databaseMetaData = con.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, dataSource, "%", null);
			while(resultSet.next()){
				NamedType namedType = new NamedType();
				namedType.setName(dataSource+"."+resultSet.getString(3));
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		} catch (SQLException e) {
			e.getMessage();
		}
		
		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(Connection con, String dataSource, String tableName) {
		DatabaseMetaData databaseMetaData;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			databaseMetaData = con.getMetaData();
			ResultSet columns = databaseMetaData.getColumns(null, null, tableName.split("\\.")[1], null);
			while (columns.next()) {
				String type = columns.getString("TYPE_NAME");
				String columnName = columns.getString("COLUMN_NAME");
				NamedType attributeForColumn = null;
				if (type.contains("array")) {
					if(type.contains("struct")){
						String modifiedType = removeParenthesis(type.replace("array","").replace("struct", "").replace("<", "").replace(">", ""), "()");
						String[] columnNamesWithTypes = modifiedType.split(",");
						List<String> list = Arrays.asList(columnNamesWithTypes);
						 for(String columnType : list ){
							 String[] columnAndDataType = columnType.split(":");
							 attributeForColumn = new NamedType();
							 attributeForColumn.setName(columnName+"."+columnAndDataType[0]);
							 Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
							            .dataType(getDataType(columnAndDataType[1].toUpperCase().trim()))
							            .nullable(columns.getString("IS_NULLABLE").equals("YES") ? true : false)
										.autoIncrement(columns.getMetaData().isAutoIncrement(1)  ? true : false);
							 attributeForColumn.setType(typeForCoumn);
							 attributeList.add(attributeForColumn);
						 }
					}else if(type.contains("string")){
						 String modifiedType = type.replace("array","").replace("<", "").replace(">", "");
						 attributeForColumn = new NamedType();
						 attributeForColumn.setName(columnName);
						 Type typeForCoumn = new Type().kind(Type.KindEnum.LIST).dataType(getDataType(modifiedType.toUpperCase()))
								    .nullable(columns.getString("IS_NULLABLE").equals("YES") ? true : false)
									.autoIncrement(columns.getMetaData().isAutoIncrement(1)  ? true : false);
						 attributeForColumn.setType(typeForCoumn);
						 attributeList.add(attributeForColumn);
					}
				}else{
					 attributeForColumn = new NamedType();
					 attributeForColumn.setName(columnName);
					 Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type.toUpperCase()))
							    .nullable(columns.getString("IS_NULLABLE").equals("YES") ? true : false)
								.autoIncrement(columns.getMetaData().isAutoIncrement(1)  ? true : false);
					 attributeForColumn.setType(typeForCoumn);
					 attributeList.add(attributeForColumn);
				}
			}
		} catch (SQLException e) {
			e.getMessage();
		}
		return attributeList;
	}
	private List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData=null;
		try {
			if (con != null) {
				databaseMetaData = con.getMetaData();
				resultSet = databaseMetaData.getCatalogs();
				while (resultSet.next()) {
					List<String> schemaNameList = getSchemaByDatabse(con, resultSet.getString(1));
					for (String schema : schemaNameList) {
						schemaList.add(resultSet.getString(1) + "." + schema);
					}
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		} finally {
			closeSqlObject(resultSet);
		}
		return schemaList;
	}

	private List<String> getSchemaByDatabse(Connection con, String databaseName) throws SQLException {
		List<String> schemas = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		String schemasQuery = null;
		try {
			String replacedDatabaseName = databaseName.replaceAll("\\[", "").replaceAll("\\]", "");
			schemasQuery = " SELECT  DISTINCT QUOTENAME(TABLE_SCHEMA)  FROM " + databaseName
					+ ".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG=? ";
			preparedStatement = con.prepareStatement(schemasQuery);
			preparedStatement.setString(1, replacedDatabaseName);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				schemas.add(resultSet.getString(1));
			}
		} finally {
			closeSqlObject(resultSet, preparedStatement);
		}

		return schemas;
	}

 
	private int getTableCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {

		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			countRows = getCountRows(connection, tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			closeSqlObject(connection);
		}
		return countRows;
	}
	
	
	/* removing parenthesis and everything inside them, works for (),[] and {} */
	
	private  String removeParenthesis(String input_string, String parenthesis_symbol)
	{
		if (parenthesis_symbol.contains("[]")) {
			return input_string.replaceAll("\\s*\\[[^\\]]*\\]\\s*", " ");
		} else if (parenthesis_symbol.contains("{}")) {
			return input_string.replaceAll("\\s*\\{[^\\}]*\\}\\s*", " ");
		} else {
			return input_string.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ");
		}
	}
	
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec ,int containersCount) throws DataExtractionServiceException 
	{
		StartResult startResult = null;
		try 
		{
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
		
		if (databaseTable.length == 2 ) 
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

			String dataSourceScope, String dataSourceSampleSize) throws IOException {

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