package com.prifender.des.adapter.hierarchical.couchbase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataExtractionUtil;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;
import com.prifender.des.model.Type.DataTypeEnum;
import com.prifender.des.model.Type.KindEnum;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component
public final class CouchBaseDataSourceAdapter implements DataSourceAdapter {

    @Value( "${des.home}" )
    private String desHome;
    @Autowired
	EncryptionServiceClient encryptionServiceClient;
    private static final String TYPE_ID = "Couchbase";

	static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Couchbase")

			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(
					new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(
					new ConnectionParamDef().id("BucketName").label("BucketName").type(TypeEnum.STRING))
			.addConnectionParamsItem(
					new ConnectionParamDef().id("BucketPassword").label("BucketPassword").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(
					new ConnectionParamDef().id("QueryTimeout").label("QueryTimeout").type(TypeEnum.INTEGER))
			.addConnectionParamsItem(
					new ConnectionParamDef().id("ConnectTimeout").label("ConnectTimeout").type(TypeEnum.INTEGER));

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
			userName = getConnectionParam(ds, "UserName");
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
				String bucketPassword = getConnectionParam(ds, "BucketPassword");
				String bucketName = getConnectionParam(ds, "BucketName");
				userName = getConnectionParam(ds, "UserName");
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

	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {
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
				String bucketPassword = getConnectionParam(ds, "BucketPassword");
				userName = getConnectionParam(ds, "UserName");
				countRows = getCountRows(cluster, tableName, bucketPassword);
			}
		} finally {
			destroy(userName, clusterManager, cluster, couchBaseEnviorment);
		}
		return countRows;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging)
			throws DataExtractionServiceException {
		StartResult startResult = null;
		try {
			String tableName = spec.getCollection();
			int dataSize = getCountRows(ds, tableName);
			if (dataSize == 0) {
				throw new DataExtractionServiceException(
						new Problem().code("meta data error").message("No Rows Found in bucket :" + tableName));
			}
			String[] schemaTableName = StringUtils.split(tableName, "." );
			tableName = schemaTableName[schemaTableName.length-1];
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new CouchBaseDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
				executor.execute(dataExtractionExecutor);
				startResult = new StartResult(job, dataExtractionExecutor);
			} catch (Exception err) {
				throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
			} finally {
				executor.shutdown();
				executor.awaitTermination(1, TimeUnit.SECONDS);
			}

		} catch (InterruptedException err) {
			throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
		}
		return startResult;
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

				List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), clusterManager,
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

		final String hostName = getConnectionParam(ds, "Host");
		final String portNumber = getConnectionParam(ds, "Port");
		final String userName = getConnectionParam(ds, "UserName");
		final String password = getConnectionParam(ds, "Password");
		final int connectTimeout = Integer.parseInt(getConnectionParam(ds, "ConnectTimeout"));
		final int queryTimeout = Integer.parseInt(getConnectionParam(ds, "QueryTimeout"));

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

	protected static final String getConnectionParam(final DataSource ds, final String param) {
		if (ds == null) {
			throw new IllegalArgumentException();
		}

		if (param == null) {
			throw new IllegalArgumentException();
		}

		for (final ConnectionParam cp : ds.getConnectionParams()) {
			if (cp.getId().equals(param)) {
				return cp.getValue();
			}
		}

		return null;
	}

	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}

	private Type getTypeForColumn(String type) {
		return entryType(Type.KindEnum.VALUE).dataType(getDataType(type));
	}

}
