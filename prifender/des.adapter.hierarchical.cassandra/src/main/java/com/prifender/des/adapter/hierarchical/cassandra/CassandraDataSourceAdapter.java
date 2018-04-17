package com.prifender.des.adapter.hierarchical.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
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
import com.prifender.des.util.DatabaseUtil;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component
public class CassandraDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value("${des.home}")
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "Cassandra";
	
	
	static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Cassandra")

			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(new ConnectionParamDef().id("ReadTimeout").label("ReadTimeout").type(TypeEnum.INTEGER).required(false))
			.addConnectionParamsItem(new ConnectionParamDef().id("ConnectTimeout").label("ConnectTimeout").type(TypeEnum.INTEGER).required(false))
			.addConnectionParamsItem(new ConnectionParamDef().id("Database").label("Database").type(TypeEnum.STRING));
			
	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {

		Cluster cluster = null;
		Session session = null;
		try {

			cluster = getDataBaseConnection(ds, cluster);
			final String database = getConnectionParam(ds, "Database");
			session = getSession(cluster, database);
			if (session != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS)
						.message("Cassandra connection successfully established.");
			}
		} finally {
			destroyConnections(cluster, session);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Cassandra database");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Cluster cluster = null;
		Session session = null;

		try {
			cluster = getDataBaseConnection(ds, cluster);

			final String database = getConnectionParam(ds, "Database");
			session = getSession(cluster, database);

			if (session != null) {
				metadata = metadataByConnection(session, cluster, database);
				if (metadata == null) {
					throw new DataExtractionServiceException(
							new Problem().code("metadata error").message("meta data not found for connection."));
				}
			} else {
				throw new DataExtractionServiceException(
						new Problem().code("Connection Failure").message("Could not connect to Cassandra database."));
			}
		} finally {
			destroyConnections(cluster, session);
		}
		return metadata;
	}

	@Override
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {

		Cluster cluster = null;
		Session session = null;

		cluster = getDataBaseConnection(ds, cluster);
		final String database = getConnectionParam(ds, "Database");
		session = getSession(cluster, database);

		int count = 0;
		if (session != null) {
			count = getRowCount(database, tableName, cluster, session);
		}

		return count;
	}
	
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,
			MessagingConnectionFactory messaging) throws DataExtractionServiceException {
		
		StartResult startResult = null;
		try {

			String tableName = spec.getCollection();
			int dataSize = getCountRows(ds, tableName);
			if (dataSize == 0) {
				throw new DataExtractionServiceException(new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			
			tableName = getTableName(tableName);
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new CassandraDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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
	
	private Metadata metadataByConnection(Session session,Cluster cluster,String keySpace ) throws DataExtractionServiceException {
		
		Metadata metadata = new Metadata();
		List<String> keySpaceList = new ArrayList<String>();
		
		if(StringUtils.isNotBlank(keySpace)){
			keySpaceList.add(keySpace);
		}else{
			keySpaceList =  getKeySpaceList(cluster);
		}
		
		if ( keySpaceList.size() == 0) {
			throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
		}
		
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (NamedType namedType1 : getNameTypeKeySpace(keySpaceList)) {
			
			List<NamedType> tableList = getDatasourceRelatedTables(cluster, namedType1.getName());
			
			for (NamedType namedType : tableList) {
				
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				
				String tableName = getTableName(namedType.getName());
				List<NamedType> attributeListForColumns = getTableRelatedColumns(cluster,  namedType1.getName(),tableName);
				
				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	private String getTableName(String schemaTableName){
		String tableName = "";
		if(schemaTableName.contains(".")){
			String[] schemaTableNameList = StringUtils.split(schemaTableName, "." );
			tableName = schemaTableNameList[schemaTableNameList.length-1];
		}
		return tableName;
	}
	private List<NamedType> getTableRelatedColumns(Cluster cluster, String dataSource, String name) {
		List<NamedType> columnNamedTypeList = new ArrayList<NamedType>();
		
		List<ColumnMetadata> columnMetadataList = cluster.getMetadata().getKeyspace(dataSource).getTable(name).getColumns();
		for(ColumnMetadata colMetadata:columnMetadataList ){
			
			NamedType namedType = new NamedType();
			namedType.setName(colMetadata.getName());
			String type = colMetadata.getType().toString();
			
			if(type.contains("set<text>")){
				namedType.setType(new Type().kind(Type.KindEnum.LIST));
				
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = 	getSubColumnRelatedValues(colMetadata.getType());
				entryType.setAttributes(attributeListForColumns);
				columnNamedTypeList.add(namedType);
				
			}else if(type.contains("map")){
				//namedType.setType(new Type().kind(Type.KindEnum.MAP));
				
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				
				List<NamedType> attributeListForColumns = 	getSubColumnRelatedValues(colMetadata.getType());
				entryType.setAttributes(attributeListForColumns);
				columnNamedTypeList.add(namedType);
				
			}else if(type.contains("list")){
				
				namedType.setType(new Type().kind(Type.KindEnum.LIST));
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				
				
				List<NamedType> attributeListForColumns = 	getSubColumnRelatedValues(colMetadata.getType());
				entryType.setAttributes(attributeListForColumns);
				columnNamedTypeList.add(namedType);
				
			}else{
				
				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
						.dataType(databaseUtilService.getDataType(type.toUpperCase()));
				namedType.setType(typeForCoumn);
				columnNamedTypeList.add(namedType);
			}
			
		}
		return columnNamedTypeList;
	}

	
	@SuppressWarnings("static-access")
	private List<NamedType> getSubColumnRelatedValues(DataType elementType){
		
		List<NamedType> namedTypeList = new ArrayList<NamedType> ();
		
		if(elementType.getTypeArguments().size() > 0){
			String type = elementType.getTypeArguments().get(0).getClass().getName().toString();
			if(type.contains("UserType")){
				
				UserType userType = (UserType) elementType.list(elementType).getTypeArguments().iterator().next().getTypeArguments().iterator().next();
				Collection<String> subColDataList = userType.getFieldNames();
				
				for(String subColData:subColDataList){
					NamedType namedType = new NamedType();
					namedType.setName(subColData);
					Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(userType.getFieldType(subColData).toString().toUpperCase()));
					namedType.setType(columnType);
					namedTypeList.add(namedType);
				}
			}else {
				List<DataType>  dst = elementType.getTypeArguments();
				for(DataType ds:dst){
					NamedType namedType = new NamedType();
					String  subType = ds.getName().toString().toUpperCase();
					Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(subType));
					namedType.setType(columnType);
					namedTypeList.add(namedType);
				}
			}
		}else{
			NamedType namedType = new NamedType();
			String  subType = elementType.getName().getClass().toString().toUpperCase();
			Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(subType));
			namedType.setType(columnType);
			namedTypeList.add(namedType);
		}
		
		return namedTypeList;
	}
	 
	/*
	 * Here u will get only one Database get all the Tables of that specific database and then add the tables to the list
	 */
	private List<NamedType> getDatasourceRelatedTables(Cluster cluster, String dataSource) {
		List<NamedType> tableMetadataList = new ArrayList<NamedType>();
		
		KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(dataSource);
		
		Collection<TableMetadata> keySpaceTableList = keyspaceMetadata.getTables();
		for(TableMetadata tableName:keySpaceTableList){
			NamedType namedType = new NamedType();
			namedType.setName(dataSource+"."+tableName.getName());
			Type type = new Type().kind(Type.KindEnum.LIST);
			namedType.setType(type);
			tableMetadataList.add(namedType);
		}

		return tableMetadataList;
	}

	/*
	 * Returns all the KeySpaces for the given Cluster
	 */
	private List<String> getKeySpaceList(Cluster cluster) {
		List<String> keySpaceList = new ArrayList<String>();
		List<KeyspaceMetadata> keySpaceMetadataList = cluster.getMetadata().getKeyspaces();
		for(KeyspaceMetadata keySpaceMetadata:keySpaceMetadataList){
			keySpaceList.add(keySpaceMetadata.getName());
		}
		return keySpaceList;
	}

	private List<NamedType> getNameTypeKeySpace(List<String> keySpaceList) {
		List<NamedType> namedTypeList = new ArrayList<NamedType>();
		
		for (String keySpace : keySpaceList) {
			NamedType namedType = new NamedType();
			namedType.setName(keySpace);
			Type type = new Type().kind(Type.KindEnum.LIST);
			namedType.setType(type);
			namedTypeList.add(namedType);
		}
		
		return namedTypeList;
	}
	
	

	private int getRowCount(String database, String tableName, Cluster cluster, Session session) {
		
		int count  = 0 ;
	
		String query = "Select Count (*) from " +  tableName;
		ResultSet results = session.execute(query);
		while(results.iterator().hasNext()){
			Row row = results.one();
			if(row != null){
			count = (int) row.getLong(0);
			}
		}
		return count;
	}

	private Cluster getDataBaseConnection(DataSource ds,Cluster cluster) throws DataExtractionServiceException{
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		final String host = getConnectionParam(ds, "Host");
		final String userName = getConnectionParam(ds, "UserName");
		final String password = getConnectionParam(ds, "Password");
		final int readTimeout = Integer.valueOf(getConnectionParam(ds, "ReadTimeout"));
		final int connectTimeout = Integer.valueOf(getConnectionParam(ds, "ConnectTimeout"));

		return getDataBaseConnection(host, userName, password,readTimeout,connectTimeout,cluster);
	}
	
	private Cluster getDataBaseConnection(String host,String userName,String password,int readTimeout,int connectTimeout,
			 Cluster cluster) {

		AuthProvider authProvider = new PlainTextAuthProvider(userName, password);
/*		QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL);
		SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(readTimeout).setConnectTimeoutMillis(connectTimeout);*/
		cluster = Cluster.builder().addContactPoint(host).withAuthProvider(authProvider)
				/*.withSocketOptions(socketOptions).withQueryOptions(queryOptions)*/.build();
		return cluster;
	}

	private Session getSession(Cluster cluster,String database) {
		Session session = cluster.connect();
		return session;
	}
	
	private void destroyConnections(Cluster cluster, Session session) {
		if (cluster != null ) {
			cluster.close();
		}
		if(session != null){
			session.close();
		}
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
}
