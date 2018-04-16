package com.prifender.des.adapter.nonrelational.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
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
@SuppressWarnings("rawtypes")
public final class MongoDataSourceAdapter implements DataSourceAdapter {
    
    @Value( "${des.home}" )
    private String desHome;
    @Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "Mongo";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Mongo")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("DatabaseName").label("DatabaseName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD));

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	public void destroy(MongoClient mongoClient) {
		if (mongoClient != null) {
			try {
				mongoClient.close();
				mongoClient = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		MongoClient mongoClient = null;
		try {
			mongoClient = getDataBaseConnection(ds);
			if (mongoClient != null) {
				try {
					MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
					if (dbsCursor != null) {
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS)
								.message("Mongo connection successfully established.");
					}
				} catch (MongoSecurityException e) {
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
							.message(e.getMessage());
				} catch (MongoTimeoutException timeout) {
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
							.message(timeout.getMessage());
				} catch (MongoException e) {
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
							.message(e.getMessage());
				}
			}
		} finally {
			destroy(mongoClient);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Mongo database");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		MongoClient mongoClient = null;
		Metadata metadata = null;
		try {
			mongoClient = getDataBaseConnection(ds);
			if (mongoClient != null) {
				final String dataSourceName = getConnectionParam(ds, "DatabaseName");
				try {
					MongoCursor<String> dbsCursor = mongoClient.listDatabaseNames().iterator();
					if (dbsCursor != null) {
						metadata = metadataByConnection(mongoClient, dataSourceName);
						if (metadata == null) {
							throw new DataExtractionServiceException(new Problem().code("metadata error")
									.message("meta data not found for connection."));
						}
					}
				} catch (MongoSecurityException e) {
					throw new DataExtractionServiceException(
							new Problem().code("ERROR: Authentication Failed.").message(e.getMessage()));
				} catch (MongoTimeoutException timeout) {
					throw new DataExtractionServiceException(
							new Problem().code("ERROR: Authentication Failed.").message(timeout.getMessage()));
				}catch(MongoException me){
					throw new DataExtractionServiceException(
							new Problem().code("ERROR: Authentication Failed.").message(me.getMessage()));
				}
			}
		} finally {
			destroy(mongoClient);
		}
		return metadata;
	}

	@Override
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		MongoClient mongoClient = null;
		try {
			mongoClient = getDataBaseConnection(ds);
			if (mongoClient != null) {
				String databaseName = getConnectionParam(ds, "DatabaseName");
				String[] dataSourceDocumet = StringUtils.split(tableName, ".");
				if (dataSourceDocumet.length == 2 ) {
					databaseName = dataSourceDocumet[0];
					tableName = dataSourceDocumet[1];
				}
				countRows = getCountRows(tableName, mongoClient, databaseName);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(mongoClient);
		}
		return countRows;

	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging )
			throws DataExtractionServiceException {
		StartResult startResult = null;
		try {
			final String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
			String tableName = spec.getCollection();
			int dataSize = getCountRows(ds, tableName);
			if (dataSize == 0) {
				throw new DataExtractionServiceException(
						new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			String[] schemaTableName = StringUtils.split(tableName, "." );
			tableName = schemaTableName[schemaTableName.length-1];
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				DataExtractionThread dataExtractionExecutor = new MongoDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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

	private MongoClient getDataBaseConnection(DataSource ds) throws DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		final String hostName = getConnectionParam(ds, "Host");
		final String portNumber = getConnectionParam(ds, "Port");
		final String databaseName = getConnectionParam(ds, "DatabaseName");
		final String userName = getConnectionParam(ds, "UserName");
		final String password = getConnectionParam(ds, "Password");

		return getDataBaseConnection(hostName, Integer.parseInt(portNumber), databaseName, userName, password);
	}

	@SuppressWarnings("deprecation")
	private MongoClient getDataBaseConnection(String hostName, int portNumber, String databaseName, String userName,
			String password) {
		MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
		/*optionsBuilder.connectTimeout( 20000 );
		optionsBuilder.readPreference( ReadPreference.primary() );*/
		MongoClientOptions clientOptions = optionsBuilder.build();
		ServerAddress serverAddress = new ServerAddress(hostName, portNumber);
		MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(userName, databaseName, password.toCharArray());
		
		return new MongoClient(serverAddress, Arrays.asList(mongoCredential),clientOptions);
	}

	private Metadata metadataByConnection(MongoClient mongoClient, String schemaName)
			throws DataExtractionServiceException {

		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		try {
			if (StringUtils.isNotBlank(schemaName)) {
				if (schemaName.equals("all")) {
					dataSourceList = getAllDatasourcesFromDatabase(mongoClient);
				} else {
					dataSourceList = getAllDatasourcesFromDatabase(mongoClient);
					if (dataSourceList.contains(schemaName)) {
						dataSourceList = new ArrayList<String>();
						dataSourceList.add(schemaName);
					} else {
						throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
					}
				}
			} else {
				throw new DataExtractionServiceException(new Problem().code("unknown database").message("database not found in connection params."));
			}
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String dataSource : dataSourceList) {
				List<NamedType> tableList = getDatasourceRelatedTables(dataSource, mongoClient);
				for (NamedType namedType : tableList) {
					if (!StringUtils.equals(namedType.getName().toString(), dataSource+".system")
							&& !StringUtils.equals(namedType.getName().toString(), dataSource+".system.users")
							&& !StringUtils.equals(namedType.getName().toString(), dataSource+".system.version")) {
					Type entryType = entryType(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), mongoClient);

					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					metadata.setObjects(namedTypeObjectsList);
					}
				}
			}
		}catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}
		return metadata;
	}

	private List<String> getAllDatasourcesFromDatabase(MongoClient mongoClient) throws DataExtractionServiceException {
		List<String> schemaList = null;
		MongoCursor<String> dbsCursor = null;
		try {
			dbsCursor = mongoClient.listDatabaseNames().iterator();
			schemaList = new ArrayList<String>();
			while (dbsCursor.hasNext()) {
				schemaList.add(dbsCursor.next());
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Database Error").message(e.getMessage()));
		} finally {
			dbsCursor.close();
		}
		return schemaList;
	}

	private List<NamedType> getDatasourceRelatedTables(String schemaName, MongoClient mongoClient)
			throws DataExtractionServiceException {
		MongoDatabase mongoDatabase = null;
		MongoCursor<Document> document = null;
		List<NamedType> tableList = new ArrayList<>();
		try {
			mongoDatabase = mongoClient.getDatabase(schemaName);
			document = mongoDatabase.listCollections().iterator();
			while (document.hasNext()) {
				NamedType namedType = new NamedType();
				namedType.setName(mongoDatabase.getName() + "." + document.next().get("name").toString());
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Database Error").message(e.getMessage()));
		}
		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(String tableName, MongoClient mongoClient)
			throws DataExtractionServiceException {
		MongoDatabase mongoDatabase = null;
		List<NamedType> namedTypeList = null;
		MongoCollection<Document> collection = null;
		try {
			String[] schemaName = tableName.split("\\.");
			mongoDatabase = mongoClient.getDatabase(schemaName[0]);
			collection = mongoDatabase.getCollection(schemaName[1]);
			MapReduceIterable iterable = getDocumentFieldNames(collection);
			List<String> fieldsSet = new ArrayList<String>();
			for (Object obj : iterable) {
				fieldsSet.add(((Document) obj).get("_id").toString());
			}
			namedTypeList = getFieldsNamesAndDataTypeList(fieldsSet, collection);
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Database Error").message(e.getMessage()));
		}
		return namedTypeList;
	}

	private MapReduceIterable getDocumentFieldNames(MongoCollection<Document> collection) {
		String map = "function() {   \n" + "for (var key in this)\n" + "{ emit(key, null); }; } ";
		String reduce = " function(key, stuff) { return null; }";
		return collection.mapReduce(map, reduce);
	}

	private List<NamedType> getFieldsNamesAndDataTypeList(List<String> fieldsSet,
			MongoCollection<Document> collection) {
		List<NamedType> namedTypeList = new ArrayList<>();
		for (String field : fieldsSet) {
			NamedType namedType = new NamedType();
			DBObject query = new BasicDBObject();
			query.put(field, new BasicDBObject("$ne", ""));
			FindIterable<Document> cursor = collection.find((Bson) query).projection(Projections.include(field))
					.limit(1);
			List<Document> documentsList = cursor.into(new LinkedList<>());
			if (documentsList.size() == 0)
				continue;
			if (documentsList.get(0).get(field) != null) {
				namedType = getDocFieldsAndValues(documentsList, field);
				namedTypeList.add(namedType);
			}
		}
		return namedTypeList;
	}

	@SuppressWarnings("unchecked")
	private NamedType getDocFieldsAndValues(List<Document> documentsList, String field) {

		NamedType namedType = new NamedType();
		String type = documentsList.get(0).get(field).getClass().getSimpleName();
		namedType.setName(field);

		if (type.equals("ArrayList") || type.equals("Object")) {

			for (Document document : documentsList) {
				List<Object> docFieldList = (ArrayList<Object>) document.get(field);

				namedType.setType(entryType(Type.KindEnum.LIST));
				Type entryType = entryType(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);

				String docType = docFieldList.get(0).getClass().getSimpleName().toString();

				if (StringUtils.equals("Document", docType)) {
					List<NamedType> attributeListForColumns = getSubDocFieldsAndValues((Document) docFieldList.get(0));
					entryType.setAttributes(attributeListForColumns);
				} else {
					namedType.setName(field);
					namedType.setType(entryType(Type.KindEnum.LIST));
					namedType.getType()
							.setEntryType(new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(docType)));
				}
			}
		} else {
			namedType.setType(getTypeForColumn(type));
		}
		return namedType;
	}

	private List<NamedType> getSubDocFieldsAndValues(Document subDocument) {
		List<NamedType> attributeListForColumns = new ArrayList<>();
		for (Map.Entry<String, Object> set : subDocument.entrySet()) {
			NamedType namedType = new NamedType();
			namedType.setName(set.getKey());
			if (set.getValue() != null) {
				Object object = subDocument.get(set.getKey());
				namedType.setType(getTypeForColumn(object.getClass().getSimpleName()));
			}
			attributeListForColumns.add(namedType);
		}
		return attributeListForColumns;
	}

	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}

	private Type getTypeForColumn(String type) {
		return entryType(Type.KindEnum.VALUE).dataType(getDataType(type));
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

	private int getCountRows(String collectionName, MongoClient mongoClient, String dataSourceName) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(dataSourceName);
		MongoCollection collection = mongoDatabase.getCollection(collectionName);
		Long collectionCount = collection.count();
		return collectionCount.intValue();
	}

}
