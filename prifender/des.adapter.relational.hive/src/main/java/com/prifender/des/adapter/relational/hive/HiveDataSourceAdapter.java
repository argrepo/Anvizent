package com.prifender.des.adapter.relational.hive;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataExtractionUtil;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.Constraint;
import com.prifender.des.model.DataExtractionAttribute;
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
public final class HiveDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	private static final String TYPE_ID = "Hive";
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Hive")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(new ConnectionParamDef().id("DatabaseName").label("DatabaseName").type(TypeEnum.STRING));

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
			databaseUtilService.closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Hive   database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");
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
			databaseUtilService.closeSqlObject(connection);
		}
		return metadata;
	}

	public Connection getDataBaseConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = databaseUtilService.getConnectionParam(ds, "Host");
		final String port = databaseUtilService.getConnectionParam(ds, "Port");
		final String userName = databaseUtilService.getConnectionParam(ds, "UserName");
		final String password = databaseUtilService.getConnectionParam(ds, "Password");
		final String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");
		return getDataBaseConnection1(hostName, port, databaseName, userName, password);
	}

	public Connection getDataBaseConnection1(String hostName, String port, String databaseName, String userName,
			String password) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "org.apache.hive.jdbc.HiveDriver";
		Class.forName(driver);
		String url = "jdbc:hive2://" +hostName + ":" + port + "/" +databaseName;
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	public Metadata metadataByConnection(Connection con, String databaseName) throws DataExtractionServiceException {
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

					// add primary keys here
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList = getTableRelatedPkInfo(con, dataSource, namedType.getName());
					if (pkConstraintList != null && pkConstraintList.size() > 0) {
						for (Constraint constraint : pkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}
					/*// add foreign keys here
					List<Constraint> fkConstraintList = getTableRelatedFkInfo(con, dataSource, namedType.getName());
					if (fkConstraintList != null) {
						for (Constraint constraint : fkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}*/
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
							            .dataType(databaseUtilService.getDataType(columnAndDataType[1].toUpperCase().trim()))
							            .nullable(columns.getString("IS_NULLABLE").equals("YES") ? true : false)
										.autoIncrement(columns.getMetaData().isAutoIncrement(1)  ? true : false);
							 attributeForColumn.setType(typeForCoumn);
							 attributeList.add(attributeForColumn);
						 }
					}else if(type.contains("string")){
						 String modifiedType = type.replace("array","").replace("<", "").replace(">", "");
						 attributeForColumn = new NamedType();
						 attributeForColumn.setName(columnName);
						 Type typeForCoumn = new Type().kind(Type.KindEnum.LIST).dataType(databaseUtilService.getDataType(modifiedType.toUpperCase()))
								    .nullable(columns.getString("IS_NULLABLE").equals("YES") ? true : false)
									.autoIncrement(columns.getMetaData().isAutoIncrement(1)  ? true : false);
						 attributeForColumn.setType(typeForCoumn);
						 attributeList.add(attributeForColumn);
					}
				}else{
					 attributeForColumn = new NamedType();
					 attributeForColumn.setName(columnName);
					 Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(type.toUpperCase()))
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
	public List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		try {
			if (con != null) {
				String shemaQuery = "SELECT quotename(name) as DBName FROM sys.sysdatabases  WHERE HAS_DBACCESS(name) = 1 and (name not in ('master','tempdb','msdb','model') and name not like 'ReportServer$%') ";
				preparedStatement = con.prepareStatement(shemaQuery);
				resultSet = preparedStatement.executeQuery();
				while (resultSet.next()) {
					List<String> schemaNameList = getSchemaByDatabse(con, resultSet.getString(1));
					for (String schema : schemaNameList) {
						schemaList.add(resultSet.getString(1) + "." + schema);
					}
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}
		return schemaList;
	}

	public List<String> getSchemaByDatabse(Connection con, String databaseName) throws SQLException {
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
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return schemas;
	}

	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {

		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			countRows = databaseUtilService.getCountRows(connection, tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			databaseUtilService.closeSqlObject(connection);
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
				throw new DataExtractionServiceException(new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			
			String[] schemaTableName = StringUtils.split(tableName, "." );
			tableName = schemaTableName[schemaTableName.length-1];
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new HiveDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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
	public static String removeParenthesis(String input_string, String parenthesis_symbol){
	    // removing parenthesis and everything inside them, works for (),[] and {}
	    if(parenthesis_symbol.contains("[]")){
	        return input_string.replaceAll("\\s*\\[[^\\]]*\\]\\s*", " ");
	    }else if(parenthesis_symbol.contains("{}")){
	        return input_string.replaceAll("\\s*\\{[^\\}]*\\}\\s*", " ");
	    }else{
	        return input_string.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ");
	    }
	}
}