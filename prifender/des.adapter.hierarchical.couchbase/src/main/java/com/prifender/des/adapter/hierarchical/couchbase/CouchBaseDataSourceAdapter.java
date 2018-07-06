package com.prifender.des.adapter.hierarchical.couchbase;

import static com.prifender.des.util.DatabaseUtil.getConvertedDate;

import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
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
public final class CouchBaseDataSourceAdapter extends DataSourceAdapter {

    @Value( "${des.home}" )
    private String desHome;
    
    private static final String JOB_NAME = "local_project.prifender_couchbase_v1_0_1.Prifender_COUCHBASE_v1";
    
	private static final String DEPENDENCY_JAR = "prifender_couchbase_v1_0_1.jar";

	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	 
	private static final int  MAX_THRESHOULD_ROWS = 200000;
	
    private static final String TYPE_ID = "Couchbase";
    public static final String TYPE_LABEL = "Couchbase";
    
    // Bucket

    public static final String PARAM_BUCKET_ID = "Bucket";
    public static final String PARAM_BUCKET_LABEL = "Bucket";
    public static final String PARAM_BUCKET_DESCRIPTION = "The name of the data bucket";
    
    public static final ConnectionParamDef PARAM_BUCKET 
        = new ConnectionParamDef().id( PARAM_BUCKET_ID ).label( PARAM_BUCKET_LABEL ).description( PARAM_BUCKET_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Bucket Password

    public static final String PARAM_BUCKET_PASSWORD_ID = "BucketPassword";
    public static final String PARAM_BUCKET_PASSWORD_LABEL = "Bucket Password";
    public static final String PARAM_BUCKET_PASSWORD_DESCRIPTION = "The password to the data bucket";
    
    public static final ConnectionParamDef PARAM_BUCKET_PASSWORD 
        = new ConnectionParamDef().id( PARAM_BUCKET_PASSWORD_ID ).label( PARAM_BUCKET_PASSWORD_LABEL ).description( PARAM_BUCKET_PASSWORD_DESCRIPTION ).type( TypeEnum.PASSWORD );
    
    // Connect Timeout

    public static final String PARAM_CONNECT_TIMEOUT_ID = "ConnectTimeout";
    public static final String PARAM_CONNECT_TIMEOUT_LABEL = "Connect Timeout";
    public static final String PARAM_CONNECT_TIMEOUT_DESCRIPTION = "The connect timeout";
    
    public static final ConnectionParamDef PARAM_CONNECT_TIMEOUT 
        = new ConnectionParamDef().id( PARAM_CONNECT_TIMEOUT_ID ).label( PARAM_CONNECT_TIMEOUT_LABEL ).description( PARAM_CONNECT_TIMEOUT_DESCRIPTION ).type( TypeEnum.INTEGER );

    // Query Timeout

    public static final String PARAM_QUERY_TIMEOUT_ID = "QueryTimeout";
    public static final String PARAM_QUERY_TIMEOUT_LABEL = "Query Timeout";
    public static final String PARAM_QUERY_TIMEOUT_DESCRIPTION = "The query timeout";
    
    public static final ConnectionParamDef PARAM_QUERY_TIMEOUT 
        = new ConnectionParamDef().id( PARAM_QUERY_TIMEOUT_ID ).label( PARAM_QUERY_TIMEOUT_LABEL ).description( PARAM_QUERY_TIMEOUT_DESCRIPTION ).type( TypeEnum.INTEGER );

    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID ).label( TYPE_LABEL )
        .addConnectionParamsItem( PARAM_HOST )
        .addConnectionParamsItem( clone( PARAM_PORT ).required( false ).defaultValue( "8091" ) )
        .addConnectionParamsItem( PARAM_USER )
        .addConnectionParamsItem( PARAM_PASSWORD )
        .addConnectionParamsItem( PARAM_BUCKET )
        .addConnectionParamsItem( PARAM_BUCKET_PASSWORD )
        .addConnectionParamsItem( clone( PARAM_CONNECT_TIMEOUT ).required( false ).defaultValue( "10000" ) )
        .addConnectionParamsItem( clone( PARAM_QUERY_TIMEOUT ).required( false ).defaultValue( "70000" ) );

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	public void destroy(String userName, ClusterManager clusterManager, Cluster cluster,
			CouchbaseEnvironment couchBaseEnviorment) {
		if (cluster != null && couchBaseEnviorment != null) {
			try {
				cluster.disconnect();
				couchBaseEnviorment.shutdown();
				clusterManager = null;
				cluster = null;
				couchBaseEnviorment = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {

		String userName = null;
		ClusterManager clusterManager = null;
		Cluster cluster = null;
		CouchbaseEnvironment couchBaseEnviorment = null;
		try {
			Object[] couchObjects = getDataBaseConnection(ds);
			couchBaseEnviorment = (CouchbaseEnvironment) couchObjects[0];
			cluster = (Cluster) couchObjects[1];
			clusterManager = (ClusterManager) couchObjects[2];
			userName = getConnectionParam(ds, PARAM_USER_ID);
			if (clusterManager != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS)
						.message("Couchbase connection successfully established.");
			}
		} finally {
			destroy(userName, clusterManager, cluster, couchBaseEnviorment);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Couchbase database");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		String userName = null;
		ClusterManager clusterManager = null;
		Cluster cluster = null;
		CouchbaseEnvironment couchBaseEnviorment = null;
		try {
			Object[] couchObjects = getDataBaseConnection(ds);
			couchBaseEnviorment = (CouchbaseEnvironment) couchObjects[0];
			cluster = (Cluster) couchObjects[1];
			clusterManager = (ClusterManager) couchObjects[2];
			if (clusterManager != null) {
				String bucketPassword = getConnectionParam(ds, PARAM_BUCKET_PASSWORD_ID);
				String bucketName = getConnectionParam(ds, PARAM_BUCKET_ID);
				userName = getConnectionParam(ds, PARAM_USER_ID);
				metadata = metadataByConnection(clusterManager, cluster, bucketName, bucketPassword);
				if (metadata == null) {
					throw new DataExtractionServiceException(
							new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} finally {
			destroy(userName, clusterManager, cluster, couchBaseEnviorment);
		}
		return metadata;
	}

	private int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		ClusterManager clusterManager = null;
		Cluster cluster = null;
		CouchbaseEnvironment couchBaseEnviorment = null;

		String userName = null;
		try {
			Object[] couchObjects = getDataBaseConnection(ds);
			couchBaseEnviorment = (CouchbaseEnvironment) couchObjects[0];
			cluster = (Cluster) couchObjects[1];
			clusterManager = (ClusterManager) couchObjects[2];
			if (clusterManager != null) {
				String bucketPassword = getConnectionParam(ds, PARAM_BUCKET_PASSWORD_ID);
				userName = getConnectionParam(ds, PARAM_USER_ID);
				countRows = getCountRows(cluster, tableName, bucketPassword);
			}
		} finally {
			destroy(userName, clusterManager, cluster, couchBaseEnviorment);
		}
		return countRows;
	}

	private int getCountRows(Cluster cluster, String bucketName, String bucketPassword) {

		int bucketSize = 0;
		N1qlQueryResult countQueryResult = getrowCountQueryResult(cluster, bucketName, bucketPassword);

		if (countQueryResult.finalSuccess()) {
			List<N1qlQueryRow> rows = countQueryResult.allRows();
			bucketSize = rows.get(0).value().getLong("Size").intValue();
		}
		return bucketSize;
	}

	private N1qlQueryResult getrowCountQueryResult(Cluster cluster, String bucketName, String bucketPassword) {
		Bucket countedBucket = cluster.openBucket(bucketName, bucketPassword);

		String rowCountQuery = "SELECT COUNT(*) AS Size  FROM " + bucketName;
		N1qlQuery dataQuery = N1qlQuery.simple(rowCountQuery);
		N1qlQueryResult countQueryResult = countedBucket.query(dataQuery);
		return countQueryResult;
	}

	private Metadata metadataByConnection(ClusterManager clusterManager, Cluster cluster, String bucketName,
			String bucketPassword) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		List<String> bucketList = null;
		try {

			bucketList = getAllDatasourcesFromDatabase(clusterManager);
			if (bucketList == null) {
				throw new DataExtractionServiceException(new Problem().code("No Buckets").message("No Buckets found in "+TYPE_ID +"database."));
			}
			if (StringUtils.isNotBlank(bucketName)) {
				if (bucketName.equals("all")) {
					bucketList = getAllDatasourcesFromDatabase(clusterManager);
				} else {
					bucketList = getAllDatasourcesFromDatabase(clusterManager);
					if (bucketList.contains(bucketName)) {
						bucketList = new ArrayList<String>();
						bucketList.add(bucketName);
					} else {
						throw new DataExtractionServiceException(new Problem().code("Unknown Bucket").message("Bucket not found in "+TYPE_ID +"database."));
					}
				}
			} else {
				throw new DataExtractionServiceException(new Problem().code("Bucket not found").message("Bucket not found in connection params."));
			}

			for (String bucket : bucketList) {

				NamedType namedType = new NamedType();
				namedType.setName(bucket);
				namedType.setType(entryType(Type.KindEnum.LIST));
				// table entry type
				Type entryType = entryType(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);

				List<NamedType> attributeListForColumns = getTableRelatedColumns(bucket, clusterManager,
						cluster, bucketPassword);
				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);

				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error").message(e.getMessage()));
		}
		return metadata;
	}

	private List<String> getAllDatasourcesFromDatabase(ClusterManager clusterManager) {

		List<String> bucketNameList = new ArrayList<>();
		List<BucketSettings> bucketsList = clusterManager.getBuckets();

		for (BucketSettings bucketSettings : bucketsList) {
			bucketNameList.add(bucketSettings.name());
		}

		return bucketNameList;
	}

	@SuppressWarnings("unchecked")
	private List<NamedType> getTableRelatedColumns(String bucketName, ClusterManager clusterManager, Cluster cluster,
			String bucketPassword) {
		List<NamedType> namedTypeList = new ArrayList<>();

		List<N1qlQueryRow> metaDataQueryRow = getMetadataQueryResults(cluster, bucketName, bucketPassword);

		for (N1qlQueryRow metaDataQuery : metaDataQueryRow) {
			HashMap<String, Object> metaData = (HashMap<String, Object>) metaDataQuery.value().toMap().get("b");
			metaData.forEach((key, value) -> {
				NamedType namedType = new NamedType();
				boolean isExist = false;
				for(NamedType mydata: namedTypeList){
					if(StringUtils.equals(mydata.getName(),key)){
						isExist = true;
						break;
					}
				}
			if(!isExist){
				namedType.setName(key);
				String type = value.getClass().getSimpleName().toString();

				if (type.equals("ArrayList")) {
					namedType.setType(new Type().kind(Type.KindEnum.LIST));
					List<NamedType> attributeListForColumns = new ArrayList<>();
					List<?> myresult = (List<?>) value;
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					String listItemType = myresult.get(myresult.size() - 1).getClass().getSimpleName().toString();

					if (listItemType.equals("HashMap")) {
						HashMap<String, Object> list1 = (HashMap<String, Object>) myresult.get(myresult.size() - 1);
						list1.forEach((key2, value2) -> {
							NamedType namedType2 = new NamedType();
							namedType2.setName(key2);
							namedType2.setType(getTypeForColumn(value2.getClass().getSimpleName().toString()));
							attributeListForColumns.add(namedType2);
						});
						entryType.setAttributes(attributeListForColumns);
					} else {
						namedType.getType().setEntryType(new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(listItemType)));
					}
				} else {
					namedType.setType(getTypeForColumn(type));
				}
				namedTypeList.add(namedType);
			}
			});
		}
		return namedTypeList;
	}

	private List<N1qlQueryRow> getMetadataQueryResults(Cluster cluster, String bucketName, String bucketPassword) {
		Bucket countedBucket = cluster.openBucket(bucketName, bucketPassword);
		countedBucket.environment().retryDelay();
		int limit = 10000;
		/* here u get  the meta data information of the  query */
		String metaDataInformation = "SELECT  b, meta(b) AS meta FROM " + bucketName + " b WHERE OBJECT_NAMES(b) IS NOT NULL Limit "+ limit ; 
		N1qlQuery dataQuery = N1qlQuery.simple(metaDataInformation);
		N1qlQueryResult metaDataQueryResult = countedBucket.query(dataQuery);
		return metaDataQueryResult.allRows();
	}

	private Object[] getDataBaseConnection(DataSource ds) throws DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final int connectTimeout = Integer.parseInt(getConnectionParam(ds, PARAM_CONNECT_TIMEOUT_ID));
		final int queryTimeout = Integer.parseInt(getConnectionParam(ds, PARAM_QUERY_TIMEOUT_ID));

		return getDataBaseConnection(hostName, Integer.valueOf(portNumber), userName, password, connectTimeout,
				queryTimeout);
	}

	private Object[] getDataBaseConnection(String hostName, int portNumber, String userName, String password,
			int connectTimeout, int queryTimeout) {
		CouchbaseEnvironment couchBaseEnviorment = DefaultCouchbaseEnvironment.builder().connectTimeout(connectTimeout)
				.queryTimeout(queryTimeout).build();
		Cluster cluster = CouchbaseCluster.create(couchBaseEnviorment, hostName);
		ClusterManager clusterManager = cluster.clusterManager(userName, password);
		couchBaseEnviorment.continuousKeepAliveEnabled();
		return new Object[] { couchBaseEnviorment, cluster, clusterManager };
	}

	private DataTypeEnum getDataType(String dataType) {
		DataTypeEnum dataTypeEnum = null;
		if (dataType.equals("INT") || dataType.equals("number") || dataType.equals("Number")
				|| dataType.equals("Integer")) {
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

	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}

	private Type getTypeForColumn(String type) {
		return entryType(Type.KindEnum.VALUE).dataType(getDataType(type));
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
			
			String[] schemaTableName = StringUtils.split(tableName, ".");
			
			tableName = schemaTableName[schemaTableName.length - 1];
			
			DataExtractionJob job = new DataExtractionJob()
					
					.id(spec.getDataSource() + "-" + tableName + "-" + getUUID())
					
					.state(DataExtractionJob.StateEnum.WAITING);

			try 
			{
				String adapterHome = createDir(this.desHome, TYPE_ID);
				
				startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
				
			} 
			catch (Exception err) 
			{
				
				throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
				
			}

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
					
					throw new DataExtractionServiceException( new Problem( ).code( "Metadata error" ).message( "sampleSize not found" ) );
					
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
			 
			final String dataSourceTableName = spec.getCollection();
			
			final String dataSourceUser = getConnectionParam(ds, PARAM_USER_ID);
			
			final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID)+":"+getConnectionParam(ds, PARAM_PORT_ID);
			
			final String dataSourcePassword = getConnectionParam(ds, PARAM_BUCKET_PASSWORD_ID);
			
			final String queryTimeout = getConnectionParam(ds, PARAM_QUERY_TIMEOUT_ID);
			
			final String connectTimeout = getConnectionParam(ds, PARAM_CONNECT_TIMEOUT_ID);
		
	 
		Map< String , String > contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser, dataSourcePassword, dataSourceTableName,
				
				getDataSourceColumnNames( ds,spec,"[*].") , dataSourceHost, queryTimeout,connectTimeout, dataSourceTableName,job.getOutputMessagingQueue() , String.valueOf(offset),
				
				String.valueOf(limit),String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE), String.valueOf(limit));;
		
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
	 
	public Map<String, String> getContextParams(String jobFilesPath,String jobName,String dataSourceUser,
			String dataSourcePassword,String dataSourceTableName,String dataSourceColumnNames,String dataSourceHost,
			String queryTimeout,String connectTimeout, String dataSourceName,
			String jobId ,String offset,String limit,String dataSourceScope, String dataSourceSampleSize
			) throws IOException {
		Map<String, String> ilParamsVals = new LinkedHashMap<>();
 
		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate( new Date( ) )); 
		
		ilParamsVals.put("FILE_PATH", jobFilesPath);
		
		ilParamsVals.put("JOB_NAME", jobName);
		
		ilParamsVals.put("DATASOURCE_USER", dataSourceUser);
		
		ilParamsVals.put("DATASOURCE_PASS", dataSourcePassword);
		
		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);
		
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		
		ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);
		
		ilParamsVals.put("QUERYTIMEOUT", queryTimeout);
		
		ilParamsVals.put("CONNECTTIMEOUT", connectTimeout);
		
		ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
		
		ilParamsVals.put("JOB_ID", jobId);
		
		ilParamsVals.put("OFFSET", offset);
		
		ilParamsVals.put("LIMIT", limit);
		
		ilParamsVals.put("SCOPE", dataSourceScope);
		
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
 

		return ilParamsVals;

	}
}
