package com.prifender.des.adapter.nosql.hbase;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.hadoop.hbase.util.Bytes;
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
 
@Component
public final class HBaseDataSourceAdapter extends DataSourceAdapter {

	@Value( "${des.home}" )
	private String desHome;
	
	private static final String JOB_NAME = "local_project.prifender_hbase_v1_0_1.Prifender_Hbase_v1";
	
	private static final String DEPENDENCY_JAR = "prifender_hbase_v1_0_1.jar";
	
	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	
	private static final int  MAX_THRESHOULD_ROWS = 200000;		
	
	private static final String TYPE_ID = "HBase";
	private static final String TYPE_LABEL = "Apache HBase";
	
	 //   Zookeeper Quorum

    public static final String PARAM_ZOOKEEPER_QUORUM_ID = "ZookeeperQuorum";
    public static final String PARAM_ZOOKEEPER_QUORUM_LABEL = "Zookeeper Quorum";
    public static final String PARAM_ZOOKEEPER_QUORUM_DESCRIPTION = "The name of the zookeeper quorum";
    
    public static final ConnectionParamDef PARAM_ZOOKEEPER_QUORUM 
        = new ConnectionParamDef().id( PARAM_ZOOKEEPER_QUORUM_ID ).label( PARAM_ZOOKEEPER_QUORUM_LABEL ).description( PARAM_ZOOKEEPER_QUORUM_DESCRIPTION ).type( TypeEnum.STRING );
    
    //  Zookeeper Property Client Port

    public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID = "ZookeeperPropertyClientPort";
    public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_LABEL = "Zookeeper property client port";
    public static final String PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_DESCRIPTION = "The port at which the clients will connect";
    
    public static final ConnectionParamDef PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT 
        = new ConnectionParamDef().id( PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID ).label( PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_LABEL ).description( PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_DESCRIPTION ).type( TypeEnum.STRING );
    
	
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL)
			.addConnectionParamsItem(PARAM_ZOOKEEPER_QUORUM)
			.addConnectionParamsItem(PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT);


	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}
 
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Hbase connection successfully established.");
			}
		} catch (IOException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(connection);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Hbase database");
	} 

	public void destroy(Connection conn) {
		if (conn != null) {
			try {
				conn.getAdmin().close();
				conn.getConfiguration().clear();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private  Connection getDataBaseConnection(DataSource ds) throws DataExtractionServiceException, IOException {
		Connection con = null;
		try{
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		final String hBaseZookeeperQuorum = getConnectionParam(ds, PARAM_ZOOKEEPER_QUORUM_ID);
		final String hBaseZookeeperPropertyClientPort = getConnectionParam(ds, PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID);
	 
		 File file = new File(".");
		 System.getProperties().put("hadoop.home.dir", file.getAbsolutePath());
		 new File("./bin").mkdirs();
		 new File("./bin/winutils.exe").createNewFile();
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", hBaseZookeeperQuorum);
		config.set("hbase.zookeeper.property.clientPort", hBaseZookeeperPropertyClientPort);
		HBaseAdmin.checkHBaseAvailable(config);
		con = ConnectionFactory.createConnection(config);
		}catch(ConnectException | UnknownHostException  une){
			throw new DataExtractionServiceException(new Problem().code("connecion error.").message(une.getMessage()));
		}catch(Exception e){
			throw new DataExtractionServiceException(new Problem().code("connecion error.").message(e.getMessage()));
		}
		return  con;
	  }
 
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;
		Metadata metadata = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) { 
				metadata = metadataByConnection(connection.getAdmin(),connection.getConfiguration());
				if (metadata == null) {
					throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (IOException e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error").message(e.getMessage()));
		} finally {
			destroy(connection);
		}
		return metadata;
	}
	@SuppressWarnings({ "resource", "deprecation" })
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) {
				AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
		        Scan scan = new Scan();
		        final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =   new LongColumnInterpreter();
			    countRows = (int) aggregationClient.rowCount(new HTable(connection.getConfiguration(), tableName), ci, scan);
			}
		} catch ( Throwable e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(connection);
		}
		return countRows;
	}
 
	private Metadata metadataByConnection(Admin hBaseAdmin,Configuration config) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		try {
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		    List<NamedType> tableList = getDatasourceRelatedTables(hBaseAdmin);

			for (NamedType namedType : tableList) {
				Type entryType = entryType(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), config);

				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}
		return metadata;
	}
	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}
	@SuppressWarnings("deprecation")
	private List<NamedType> getTableRelatedColumns(String tableName,Configuration config) throws DataExtractionServiceException, IOException {
		List<NamedType> attributeList = new ArrayList<NamedType>();
		HTable hTable = null;
		Set<String> columns = new HashSet<String>();
		try {
			 hTable=new HTable(config, tableName);
			 
			 ResultScanner results = hTable.getScanner(new Scan().setFilter(new PageFilter(1000)));
			 for(Result result:results){
	             for(KeyValue keyValue:result.list()){
	            	 columns.add(Bytes.toString(keyValue.getFamily())  + ":" +  Bytes.toString(keyValue.getQualifier()));
	            } 
	        }
			 for(String column : columns){
					NamedType attributeForColumn = new NamedType();
    				attributeForColumn.setName(column);
    				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(column.getClass().getSimpleName()));
    				attributeForColumn.setType(typeForCoumn);
    				attributeList.add(attributeForColumn);
			 }
			 
		}catch(Exception e){
			throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}finally{
			if(hTable != null){
				hTable.close();
			}
		}
		return attributeList;
	
	}
	private List<NamedType> getDatasourceRelatedTables(Admin hBaseAdmin) 	throws DataExtractionServiceException {
		List<NamedType> tableList = new ArrayList<>();
		try {
			HTableDescriptor[] tDescriptor = hBaseAdmin.listTables();
			for (int k=0; k<tDescriptor.length;k++ ){
				NamedType namedType = new NamedType();
				namedType.setName(tDescriptor[k].getNameAsString());
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Datasource Error").message(e.getMessage()));
		}
		return tableList;
	}
	private DataTypeEnum getDataType(String dataType) {
		DataTypeEnum dataTypeEnum = null;
		if (dataType.equals("INT") || dataType.equals("number") || dataType.equals("Number")
				|| dataType.equals("Integer") || dataType.equals("INTEGER")) {
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		} else if (dataType.equals("Decimal")) {
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		} else if (dataType.equals("Float") || dataType.equals("Double") || dataType.equals("Numeric")
				|| dataType.equals("Long") || dataType.equals("Real")) {
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		} else if (dataType.equals("String")) {
			dataTypeEnum = Type.DataTypeEnum.STRING;
		} else if (dataType.equals("Boolean")) {
			dataTypeEnum = Type.DataTypeEnum.BOOLEAN;
		}
		return dataTypeEnum;
	}
	
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec ,int containersCount) throws DataExtractionServiceException 
	{
		StartResult startResult = null;
		try {
			
			String tableName = spec.getCollection();
			
			int rowCount = getCountRows(ds, tableName);

			if (rowCount == 0) 
			{
				
				throw new DataExtractionServiceException( new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
				
			}
			DataExtractionJob job = new DataExtractionJob()
					
					.id(spec.getDataSource() + "-" + tableName + "-" + getUUID())
					
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
		
		final String dataSourceHost = getConnectionParam( ds , PARAM_ZOOKEEPER_QUORUM_ID );
		
		final String dataSourcePort = getConnectionParam( ds , PARAM_ZOOKEEPER_PROPERTY_CLIENT_PORT_ID );
		
		String tableName = spec.getCollection( );
		
		 
		Map< String , String > contextParams = getContextParams( adapterHome , JOB_NAME , 
				
				 tableName , getDataSourceColumnNames(ds, spec,".") , dataSourceHost , dataSourcePort , job.getOutputMessagingQueue() , String.valueOf(offset) , String.valueOf( limit )  ,
				
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
	 * @param dataSourceTableName
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
	public Map< String , String > getContextParams(String jobFilesPath, String jobName, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,
			
			String dataSourcePort,String jobId, String offset, String limit,
			
			String dataSourceScope, String dataSourceSampleSize) throws IOException
	{

		Map< String , String > ilParamsVals = new LinkedHashMap<>( );
		
		ilParamsVals.put( "JOB_STARTDATETIME" ,  getConvertedDate( new Date( ) ) );
		
		ilParamsVals.put("FILE_PATH", jobFilesPath);
		
		ilParamsVals.put("JOB_NAME", jobName);
		
		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);
		
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		
		ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);
		
		ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);
		
		ilParamsVals.put("JOB_ID", jobId);
		
		ilParamsVals.put("OFFSET", offset);
		
		ilParamsVals.put("LIMIT", limit);
		
		ilParamsVals.put("SCOPE", dataSourceScope);
		
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
		
		return ilParamsVals;

	}

}
