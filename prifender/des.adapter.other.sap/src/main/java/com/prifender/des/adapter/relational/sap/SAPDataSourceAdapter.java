package com.prifender.des.adapter.relational.sap;


import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
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
import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFieldIterator;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoRecordFieldIterator;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataProvider;

@Component
public final class SAPDataSourceAdapter extends DataSourceAdapter {
    
    @Value( "${des.home}" )
    private String desHome;
    
    private final static String JOB_NAME = "local_project.prifender_sap_v1_0_1.Prifender_SAP_v1";
    
	private final static String DEPENDENCY_JAR = "prifender_sap_v1_0_1.jar";

	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;		
	
    
    private static final String TYPE_ID = "SAP";
    public static final String TYPE_LABEL = "SAP";
    
    // Pool

    public static final String PARAM_POOL_ID = "Pool";
    public static final String PARAM_POOL_LABEL = "Pool";
    public static final String PARAM_POOL_DESCRIPTION = "The name of the pool";
    
    public static final ConnectionParamDef PARAM_POOL
        = new ConnectionParamDef().id( PARAM_POOL_ID ).label( PARAM_POOL_LABEL ).description( PARAM_POOL_DESCRIPTION ).type( TypeEnum.STRING );
    
    // MaxConnections

    public static final String PARAM_MAX_CONNECTIONS_ID = "MaxConnections";
    public static final String PARAM_MAX_CONNECTIONS_LABEL = "Max Connections";
    public static final String PARAM_MAX_CONNECTIONS_DESCRIPTION = "The maximum number of connections";
    
    public static final ConnectionParamDef PARAM_MAX_CONNECTIONS
        = new ConnectionParamDef().id( PARAM_MAX_CONNECTIONS_ID ).label( PARAM_MAX_CONNECTIONS_LABEL ).description( PARAM_MAX_CONNECTIONS_DESCRIPTION ).type( TypeEnum.INTEGER );
    
    // Client

    public static final String PARAM_CLIENT_ID = "Client";
    public static final String PARAM_CLIENT_LABEL = "Client";
    public static final String PARAM_CLIENT_DESCRIPTION = "The name of the client";
    
    public static final ConnectionParamDef PARAM_CLIENT
        = new ConnectionParamDef().id( PARAM_CLIENT_ID ).label( PARAM_CLIENT_LABEL ).description( PARAM_CLIENT_DESCRIPTION ).type( TypeEnum.STRING );
    
    // SystemNumber

    public static final String PARAM_SYSTEM_NUMBER_ID = "SystemNumber";
    public static final String PARAM_SYSTEM_NUMBER_LABEL = "System Number";
    public static final String PARAM_SYSTEM_NUMBER_DESCRIPTION = "The system number";
    
    public static final ConnectionParamDef PARAM_SYSTEM_NUMBER
        = new ConnectionParamDef().id( PARAM_SYSTEM_NUMBER_ID ).label( PARAM_SYSTEM_NUMBER_LABEL ).description( PARAM_SYSTEM_NUMBER_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Language

    public static final String PARAM_LANGUAGE_ID = "Language";
    public static final String PARAM_LANGUAGE_LABEL = "Language";
    public static final String PARAM_LANGUAGE_DESCRIPTION = "The language";
    
    public static final ConnectionParamDef PARAM_LANGUAGE
        = new ConnectionParamDef().id( PARAM_LANGUAGE_ID ).label( PARAM_LANGUAGE_LABEL ).description( PARAM_LANGUAGE_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Repository

    public static final String PARAM_REPOSITORY_ID = "Repository";
    public static final String PARAM_REPOSITORY_LABEL = "Repository";
    public static final String PARAM_REPOSITORY_DESCRIPTION = "The repository";
    
    public static final ConnectionParamDef PARAM_REPOSITORY
        = new ConnectionParamDef().id( PARAM_REPOSITORY_ID ).label( PARAM_REPOSITORY_LABEL ).description( PARAM_REPOSITORY_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Function

    public static final String PARAM_FUNCTION_ID = "Function";
    public static final String PARAM_FUNCTION_LABEL = "Function";
    public static final String PARAM_FUNCTION_DESCRIPTION = "The function";
    
    public static final ConnectionParamDef PARAM_FUNCTION
        = new ConnectionParamDef().id( PARAM_FUNCTION_ID ).label( PARAM_FUNCTION_LABEL ).description( PARAM_FUNCTION_DESCRIPTION ).type( TypeEnum.STRING );
    
    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID ).label( TYPE_LABEL )
        .addConnectionParamsItem( PARAM_HOST )
        .addConnectionParamsItem( PARAM_PORT )
        .addConnectionParamsItem( PARAM_USER )
        .addConnectionParamsItem( PARAM_PASSWORD )
        .addConnectionParamsItem( PARAM_POOL )
        .addConnectionParamsItem( PARAM_MAX_CONNECTIONS )
        .addConnectionParamsItem( PARAM_CLIENT )
        .addConnectionParamsItem( PARAM_SYSTEM_NUMBER )
        .addConnectionParamsItem( PARAM_LANGUAGE )
        .addConnectionParamsItem( PARAM_REPOSITORY )
        .addConnectionParamsItem( PARAM_FUNCTION );
 	
    private static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
    
    @Override
		public DataSourceType getDataSourceType() {
			return TYPE;
		}
	   
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		JCoDestination destination = null;
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		try {
			  destination = getDatabaseConnection(ds);
			  final String functionName = getConnectionParam(ds, PARAM_FUNCTION_ID);
			if (destination.getRepository().getFunction(functionName) != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("SAP connection successfully established.");
			}
		} catch (Exception e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(destination);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to SAP database");
	}
	
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		JCoDestination destination = null;

		Metadata metadata = new Metadata();
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		try {
			destination = getDatabaseConnection(ds);
			if (destination != null) {
				metadata = metadataByConnection(ds,destination);
				if (metadata == null) {
					throw new DataExtractionServiceException(
							new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		}  finally {
			destroy(destination);
		} 

		return metadata;
	}
		
	private JCoDestination getDatabaseConnection(DataSource ds) {

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String systemNumber = getConnectionParam(ds, PARAM_SYSTEM_NUMBER_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String client = getConnectionParam(ds, PARAM_CLIENT_ID);
		final String language = getConnectionParam(ds, PARAM_LANGUAGE_ID);

		JCoDestination destination = null;
		Properties connectProperties = new Properties();
		connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, hostName);
		connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, systemNumber);
		connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, client);
		connectProperties.setProperty(DestinationDataProvider.JCO_USER, userName);
		connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, password);
		connectProperties.setProperty(DestinationDataProvider.JCO_LANG, language);
		connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "8");
	    connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT,"8000");
	    createDataFile(ABAP_AS, "jcoDestination", connectProperties);
	    connectProperties.clear();
		try {
			destination = JCoDestinationManager.getDestination(ABAP_AS);
		    JCoContext.begin(destination);
		} catch (JCoException e) {
			e.getMessage();
		}

		return destination;
	}

	private Metadata metadataByConnection(DataSource ds,JCoDestination destination) throws DataExtractionServiceException {
		
		final String functionName = getConnectionParam(ds, PARAM_FUNCTION_ID);
		
		Metadata metadata = new Metadata();
		List<String> functionList = null;
		try {
			
			if(functionName != null){
				functionList = new ArrayList<String>();
				functionList.add(functionName);
				}else{
					throw new DataExtractionServiceException( new Problem().code("datasource not found").message("datasource not found in connection params.") );
				}
			
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			
			for(String function : functionList){
				
			  JCoFunction jcoFunction = destination.getRepository().getFunction(function);
			  List<JCoTable> jcoTableList = getJcoFunctionRelatedTables(jcoFunction);
			  for(JCoTable tableInfo : jcoTableList){
					//table entry type
				    NamedType namedType = new  NamedType();
				    
				    Type entryType = entryType(Type.KindEnum.OBJECT);
				    namedType.setName(tableInfo.getRecordMetaData().getName());
					Type type = new Type().kind(Type.KindEnum.LIST);
					namedType.setType(type);
					
					List<NamedType> attributeListForColumns = getJcoTableRelatedColumns(tableInfo);
					
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					namedType.getType().setEntryType(entryType);
					metadata.setObjects(namedTypeObjectsList);
				}
			  }
		  } catch (Exception e) {
			 throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}
		return metadata;
	}

	private List<JCoTable> getJcoFunctionRelatedTables(JCoFunction jcoFunction) {
		JCoFieldIterator codes = jcoFunction.getTableParameterList().getFieldIterator();
		List<JCoTable> jcoTableList = new ArrayList<JCoTable>();
		while (codes.hasNextField()) {
			JCoTable tbl = codes.nextField().getTable();
			jcoTableList.add(tbl);
		}
		return jcoTableList;
	}
	
	public void destroy(JCoDestination destination){
		if (destination != null) {
			try {
				// destination.getRepository().clear();
				JCoContext.end(destination);
			} catch (JCoException e) {
				e.printStackTrace();
			}
		}
	}
	
	private List<NamedType> getJcoTableRelatedColumns(JCoTable tableInfo) throws DataExtractionServiceException {

		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			if (tableInfo.getRecordMetaData().getFieldCount() > 0) {
				do {
					for (JCoRecordFieldIterator rcdFld = tableInfo.getRecordFieldIterator(); rcdFld.hasNextField();) {
						NamedType attributeForColumn = new NamedType();
						JCoField tabField = rcdFld.nextField();
						attributeForColumn.setName(tabField.getName());
						Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(tabField.getTypeAsString()));
						attributeForColumn.setType(typeForCoumn);
						attributeList.add(attributeForColumn);
					}
				} while (tableInfo.nextRow() == true);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(
					new Problem().code("columns getting error").message(e.getMessage()));
		}
		return attributeList;
	}
	
	
	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}
	
	private DataTypeEnum getDataType(String dataType){
		DataTypeEnum dataTypeEnum = null;
		if(dataType.equals("INT") || dataType.equals("INT1") || dataType.equals("INT2")){
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		}else if(dataType.equals("BCD")){
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		}else if(dataType.equals("FLOAT")){
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		}else if(dataType.equals("CHAR") || dataType.equals("NUM") || dataType.equals("STRING") || dataType.equals("BYTE") ||  dataType.equals("XSTRING")){
			dataTypeEnum = Type.DataTypeEnum.STRING;
		} else if(dataType.equals("DATE")){
			  dataTypeEnum = Type.DataTypeEnum.DATE;
		}else if(dataType.equals("TIME")){
			  dataTypeEnum = Type.DataTypeEnum.TIME;
		}
		return dataTypeEnum;
	}

	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		JCoDestination destination = null;
		try {
			destination = getDatabaseConnection(ds);
			if (destination != null) {

				final String functionName = getConnectionParam(ds, PARAM_FUNCTION_ID);
				JCoFunction jcoFunction = destination.getRepository().getFunction(functionName);
			    if (jcoFunction != null) {
					jcoFunction.execute(destination);
				} 	
				JCoFieldIterator jCoFieldIterator = jcoFunction.getTableParameterList().getFieldIterator();
				boolean isTableFound = false;
				while (jCoFieldIterator.hasNextField()) {
					    JCoField jCoField = jCoFieldIterator.nextField();
					    JCoTable jcoTable = jCoField.getTable();
						if(jcoTable.getRecordMetaData().getName().equals(tableName)){
					        	  JCoTable tableInfo = (JCoTable)jCoField.getValue();
					        	  countRows = tableInfo.getNumRows();
					        	  isTableFound = true;
					        	  break;
						}
				}
				if (countRows <= 0 ) {
					throw new DataExtractionServiceException(new Problem().code("No Records Found").message(" Table name "+tableName+" is not found for the given Function "+functionName));
				}
	            if(!isTableFound){
					throw new DataExtractionServiceException(new Problem().code("TableName not Found").message(" Table name "+tableName+" is not found for the given Function "+functionName));
				} 
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(destination);
		} 
		return countRows;
	} 
	 static void createDataFile(String name, String suffix, Properties properties)
	    {
	        File cfg = new File(name+"."+suffix );
	            try
	            {
	                FileOutputStream fos = new FileOutputStream(cfg, false);
	                properties.store(fos, "for tests only !");
	                fos.close();
	            }
	            catch (Exception e)
	            {
	                throw new RuntimeException("Unable to create the destination file " + cfg.getName(), e);
	            }
	    }
	 @Override
		public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec ,int containersCount) throws DataExtractionServiceException 
		{
			StartResult startResult = null;
			try {
				
				final String tableName = spec.getCollection();
				
				int rowCount =  getCountRows(ds, tableName) ;
				
				if (rowCount != 0) 
				{
					DataExtractionJob job = new DataExtractionJob()
							
							.id(spec.getDataSource() + "-" + tableName + "-" + getUUID( ))
							
							.state(DataExtractionJob.StateEnum.WAITING);
							 
					 
						String adapterHome =  createDir(this.desHome, TYPE_ID);
						
						startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
				}
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
				
				DataExtractionJob job,int rowCount ,String adapterHome , int containersCount) throws DataExtractionServiceException
		{
			
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
				 
	           synchronized (job) 
	           {
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
				
			final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID);
			
			final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);
			
			final String dataSourceUser =  getConnectionParam(ds, PARAM_USER_ID);
			
			final String dataSourcePassword =  getConnectionParam(ds, PARAM_PASSWORD_ID);
			
			final String client = getConnectionParam(ds, PARAM_CLIENT_ID);
			
			final String language = getConnectionParam(ds, PARAM_LANGUAGE_ID);
			
			final String systsemNumber = getConnectionParam(ds, PARAM_SYSTEM_NUMBER_ID);
	 
			Map< String , String > contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,
					
					dataSourcePassword, spec.getCollection(), getDataSourceColumnNames(ds, spec,"."), dataSourceHost, dataSourcePort,
					
					dataSourceUser, job.getId(), String.valueOf(offset), String.valueOf(limit), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
					
					String.valueOf(limit),client,language,systsemNumber);
			
			dataExtractionTask.taskId( "DES-Task-"+getUUID( ))
							
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
		 * @param client
		 * @param language
		 * @param systsemNumber
		 * @return
		 * @throws IOException
		 */
		public Map<String, String> getContextParams ( String jobFilesPath,String jobName,String dataSourceUser, String dataSourcePassword,
				
				String dataSourceTableName,String dataSourceColumnNames,String dataSourceHost,String dataSourcePort, String dataSourceName ,
				
				String jobId ,String offset,String limit,String dataSourceScope, String dataSourceSampleSize,String client,String language,String systsemNumber
				
				) throws IOException 
		{
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
			
			ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
			
			ilParamsVals.put("JOB_ID", jobId);
			
			ilParamsVals.put("OFFSET", offset);
			
			ilParamsVals.put("LIMIT", limit);
			
			ilParamsVals.put("SCOPE", dataSourceScope);
			
			ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
			
			ilParamsVals.put("CLIENT", client);
			
			ilParamsVals.put("LANGUAGE", language);
			
			ilParamsVals.put("SYSTEM_NUMBER", systsemNumber);
	 

			return ilParamsVals;

		}
}
