package com.prifender.des.adapter.hierarchical.hdfsparquet;

import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
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

@Component
public final class HDFSParquetDataSourceAdapter extends DataSourceAdapter {

	 
	@Value( "${des.home}" )
	private String desHome;
	
    private static final String JOB_NAME = "local_project.prifender_hdfs_parquet_v1_0_1.Prifender_HDFS_Parquet_v1";
	
	private static final String DEPENDENCY_JAR = "prifender_hdfs_parquet_v1_0_1.jar";
	
	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;	
	
	public static final String TYPE_ID = "HDFSParquet";
	public static final String TYPE_LABEL = "Apache Parquet";
	// File

    public static final String PARAM_FILE_ID = "File";
    public static final String PARAM_FILE_LABEL = "File";
    public static final String PARAM_FILE_DESCRIPTION = "The path of the file";
    
    public static final ConnectionParamDef PARAM_FILE 
        = new ConnectionParamDef().id( PARAM_FILE_ID ).label( PARAM_FILE_LABEL ).description( PARAM_FILE_DESCRIPTION ).type( TypeEnum.STRING );
    
   // Dfs( Distributed File System ) Replication

    public static final String PARAM_DFS_REPLICATION_ID = "DfsReplication";
    public static final String PARAM_DFS_REPLICATION_LABEL = "Dfs( Distributed File System ) Replication";
    public static final String PARAM_DFS_REPLICATION_DESCRIPTION = "The dfs replication for File System";
    
    public static final ConnectionParamDef PARAM_DFS_REPLICATION
        = new ConnectionParamDef().id( PARAM_DFS_REPLICATION_ID ).label( PARAM_DFS_REPLICATION_LABEL ).description( PARAM_DFS_REPLICATION_DESCRIPTION ).type( TypeEnum.INTEGER );
    
	
	Configuration config;
	
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL)
			.addConnectionParamsItem(PARAM_HOST)
			.addConnectionParamsItem(PARAM_PORT)
			.addConnectionParamsItem(PARAM_USER)
			.addConnectionParamsItem(PARAM_DFS_REPLICATION)
	        .addConnectionParamsItem(PARAM_FILE);


	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Path path = null;
		try {
			path = getDataBaseConnection(ds);
			if (path != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS Parquet connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}catch (IOException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(config);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS Avro.");
	}
	
	public void destroy(Configuration config){
		config.clear();
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Path path = null;
		try {
				path = getDataBaseConnection(ds);
				final String filePath = getConnectionParam(ds, "FilePath");
				metadata = metadataByConnection(path,filePath);
			if (metadata == null) {
				throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(config);
		}
		return metadata;
	}

	private Path getDataBaseConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException, IOException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String dfsReplication = getConnectionParam(ds, PARAM_DFS_REPLICATION_ID);
		final String filePath = getConnectionParam(ds, PARAM_FILE_ID);
		return getDataBaseConnection(hostName, portNumber, dfsReplication, filePath);
	}

	public Path getDataBaseConnection(String hostName, String port, String dfsReplication, String filePath )
			throws SQLException, ClassNotFoundException, IOException {

		File file = new File(".");
		System.getProperties().put("hadoop.home.dir", file.getAbsolutePath());
		new File("./bin").mkdirs();
		new File("./bin/winutils.exe").createNewFile();

		config = new Configuration();
		config.set("fs.defaultFS", "hdfs://" + hostName);
		config.setInt("dfs.replication", Integer.valueOf(dfsReplication));
		config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		Path path = new Path(filePath);
		return path;
	}

	private Metadata metadataByConnection(Path path,String fileName) throws DataExtractionServiceException, IOException {
		Metadata metadata = new Metadata();
		try { 
			if(path == null){
				throw new DataExtractionServiceException(
						new Problem().code("path not founnd").message("File not found in the given path."));
			}
			  ParquetMetadata readFooter = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
	          MessageType schema = readFooter.getFileMetaData().getSchema();
			  List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
				List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
				for (NamedType namedType : fNameHeaderList) {
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getColumnsFromFile(schema);
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
		    
		} catch (IOException  e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		} 
		return metadata;
	}
	private List<NamedType> getFileNameFromList(String fName) {
		List<NamedType> tableList = new ArrayList<>();
		NamedType namedType = new NamedType();
		namedType.setName(fName);
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		tableList.add(namedType);
		return tableList;
	} 
	
	private List<NamedType> getColumnsFromFile(MessageType schema) throws DataExtractionServiceException {
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
		     
		    List<org.apache.parquet.schema.Type> list = schema.getFields();
             Map<String,String> map = new HashMap<String,String>();
             List<ColumnDescriptor> columnDescriptorList = schema.getColumns();
             for(ColumnDescriptor columnDescriptor : columnDescriptorList){
            	 String s = columnDescriptor.toString();
            	  for(org.apache.parquet.schema.Type type : list){
                     if(s.contains(type.getName())){
                    	 if(s.contains("bag")){
                    		 String result = columnDescriptor.toString().substring(0, columnDescriptor.toString().indexOf("]"));
                    		 String[] names = result .split(",");
                    		 String name  = names[names.length-1];
                    		 map.put(type.getName().toString()+"."+name.trim(), columnDescriptor.getType().toString());
                    	 }else{
                    		 map.put(type.getName().toString(), columnDescriptor.getType().toString());
                    	 }
                      }
                    }  
                 } 
             for (Map.Entry<String,String> entry : map.entrySet()) {
                NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(entry.getKey());
				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(entry.getValue().toUpperCase()));
				attributeForColumn.setType(typeForCoumn);
				attributeList.add(attributeForColumn);
              }
               
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}

		return attributeList;
	}
	 
	private int getTableCountRows(DataSource ds, String tableName) throws DataExtractionServiceException, IOException 
	{
		int countRows = 0;
		Path path = null;
		ParquetFileReader parquetFileReader=null;
		try 
		{
			path = getDataBaseConnection(ds);
			ParquetMetadata readFooter = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
            parquetFileReader = new ParquetFileReader(config, path, readFooter);
            PageReadStore pages = null;
            while (null != (pages = parquetFileReader.readNextRowGroup())) {
                 countRows = (int) pages.getRowCount();
            }
            parquetFileReader.close();
           
		} 
		catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} 
		catch (IOException e1) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e1.getMessage()));
		} finally {
			if(parquetFileReader != null){
				parquetFileReader.close();
			}
			destroy(config);
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
			
			final String hdfsFilePath = getConnectionParam(ds, PARAM_FILE_ID);
		 
 
		Map< String , String > contextParams = getContextParams( adapterHome , JOB_NAME , dataSourceUser ,
				
				hdfsFilePath , getDataSourceColumnNames(ds, spec,".") , "hdfs://"+dataSourceHost , dataSourcePort ,
				
				 job.getOutputMessagingQueue() , String.valueOf(offset) , String.valueOf( limit )  ,
				
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
	 * @param hdfsUserName
	 * @param hdfsFilePath
	 * @param dataSourceColumnNames
	 * @param dataSourceHost
	 * @param dataSourcePort
	 * @param jobId
	 * @param offset
	 * @param limit
	 * @param dataSourceScope
	 * @param dataSourceSampleSize
	 * @return
	 * @throws IOException
	 */
	public Map<String, String> getContextParams(String jobFilesPath,String jobName,String hdfsUserName,String hdfsFilePath,String dataSourceColumnNames,String dataSourceHost,
			
			String dataSourcePort, String jobId ,String offset,String limit,String dataSourceScope, String dataSourceSampleSize) throws IOException {

		Map< String , String > ilParamsVals = new LinkedHashMap<>( );
		
		ilParamsVals.put( "JOB_STARTDATETIME" , getConvertedDate( new Date( ) ) );
		
		ilParamsVals.put( "FILE_PATH" , jobFilesPath );
		
		ilParamsVals.put( "JOB_NAME" , jobName );
		
		ilParamsVals.put("HDFS_URI", dataSourceHost); 
		
		ilParamsVals.put("HADOOP_USER_NAME", hdfsUserName);
		
		ilParamsVals.put("HDFS_FILE_PATH", hdfsFilePath);
		
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		
		ilParamsVals.put("JOB_ID", jobId);
		
		ilParamsVals.put("OFFSET", offset);
		
		ilParamsVals.put("LIMIT", limit);
		
		ilParamsVals.put("SCOPE", dataSourceScope);
		
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
		
		return ilParamsVals;

	}

}