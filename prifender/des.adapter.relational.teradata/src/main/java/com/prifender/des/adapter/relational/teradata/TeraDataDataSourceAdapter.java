package com.prifender.des.adapter.relational.teradata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
public final class TeraDataDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "Teradata";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Teradata")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.INTEGER))
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
						.message("Teradata connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			databaseUtilService.closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to Teradata");
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
		final String userName = databaseUtilService.getConnectionParam(ds, "UserName");
		final String password = databaseUtilService.getConnectionParam(ds, "Password");
		final String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");
		final String portNumber = databaseUtilService.getConnectionParam(ds, "Port");
		return getDataBaseConnection(hostName,portNumber, databaseName, userName, password);
	}

	public Connection getDataBaseConnection(String hostName, String portNumber,  String databaseName, String userName,
			String password) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "com.teradata.jdbc.TeraDriver";
		String url = "jdbc:teradata://" + hostName+ "/" + ( databaseName != null ? "database=" + databaseName : "" );
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	public Metadata metadataByConnection(Connection con, String databaseName) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> databaseList = null;
		try {
			databaseList = getAllDatasourcesFromDatabase(con);
			if ( databaseList.size() == 0) {
				throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
			}
			if (databaseList.size() > 0) {
				if (databaseName != null) {
					if (databaseList.contains(databaseName)) {
						databaseList = new ArrayList<String>();
						databaseList.add(databaseName);
					} else {
						throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
					}
				}
			}  
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String dataSource : databaseList) {
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
					if (pkConstraintList != null) {
						for (Constraint constraint : pkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}
					// add foreign keys here
					List<Constraint> fkConstraintList = getTableRelatedFkInfo(con, dataSource, namedType.getName());
					if (fkConstraintList != null) {
						for (Constraint constraint : fkConstraintList) {
							pkFkConstraintList.add(constraint);
						}
					}
					entryType.setConstraints(pkFkConstraintList);
					namedType.setName(dataSource + "." + namedType.getName());
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (DataExtractionServiceException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}
		return metadata;
	}

	private List<NamedType> getSchemaRelatedTables(Connection con, String dataSource) throws DataExtractionServiceException {
		DatabaseMetaData databaseMetaData;
		List<NamedType> tableList = new ArrayList<>();
		ResultSet resultSet = null;
		try {
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getTables(null, dataSource, "%", null);
			while(resultSet.next()){
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString("TABLE_NAME"));
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		} catch (SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}finally{
			databaseUtilService.closeSqlObject(resultSet);
		}
		
		return tableList;
	}

	private List<NamedType> getTableRelatedColumns(Connection con, String dataSource, String tableName) throws DataExtractionServiceException {
		DatabaseMetaData databaseMetaData;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		ResultSet resultSet=null;
		try {
			databaseMetaData = con.getMetaData();
			resultSet = databaseMetaData.getColumns(null, dataSource, tableName, null);
			while (resultSet.next()) {
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(resultSet.getString("COLUMN_NAME"));
				Type columnInfo = getColumnInfo(con,dataSource,tableName,resultSet.getString(1));
				if(columnInfo != null){ 
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
												   .dataType(databaseUtilService.getDataType(resultSet.getString("TYPE_NAME").toUpperCase())) 
												   .nullable(columnInfo.getNullable())
												   .autoIncrement(columnInfo.getAutoIncrement())
												   .size(columnInfo.getSize());
				    attributeForColumn.setType(typeForCoumn);
					attributeList.add(attributeForColumn);
				 }
			}
		} catch (SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}finally{
			databaseUtilService.closeSqlObject(resultSet);
		}

		return attributeList;
	}
	public Type getColumnInfo(Connection connection,String dataBaseName,String tableName,String columnName) throws DataExtractionServiceException{
		Type type= new Type();
		ResultSet resultSet = null;
		try {
			DatabaseMetaData meta = connection.getMetaData();
			resultSet = meta.getColumns(null, dataBaseName, tableName,columnName);
			if (resultSet.next()) {
				type.setSize(resultSet.getInt("COLUMN_SIZE"));
				type.setAutoIncrement(resultSet.getMetaData().isAutoIncrement(1)  ?  true  :  false );
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false); 
			} 
		} catch (SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}finally{
			databaseUtilService.closeSqlObject(resultSet);
		}
		return type;
	}
	private List<Constraint> getTableRelatedPkInfo(Connection con, String dataSource, String tableName) {
		DatabaseMetaData databaseMetaData;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		ResultSet primaryKeysResultSet = null;
		try {
			databaseMetaData = con.getMetaData();
			primaryKeysResultSet = databaseMetaData.getPrimaryKeys(null, dataSource, tableName);
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
		}finally{
			databaseUtilService.closeSqlObject(primaryKeysResultSet);
		}

		return constraintList;
	}
	
	private List<Constraint> getTableRelatedFkInfo(Connection con, String dataSource, String tableName) {
		List<Constraint> constraintList = new ArrayList<Constraint>();
		ResultSet primaryKeysResultSet=null;
		try {
			DatabaseMetaData databaseMetaData = con.getMetaData();
			primaryKeysResultSet = databaseMetaData.getImportedKeys(null,dataSource,tableName);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (primaryKeysResultSet.next()) {
					pkAttributes.add(primaryKeysResultSet.getString("FKCOLUMN_NAME"));
			}
			if (pkAttributes != null && pkAttributes.size() > 0) {
				constraint.kind(Constraint.KindEnum.FOREIGN_KEY);
				constraint.setAttributes(pkAttributes);
				constraintList.add(constraint);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}finally{
			databaseUtilService.closeSqlObject(primaryKeysResultSet);
		}

		return constraintList;
	}
	
	public List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		try {
			if (con != null) {
				DatabaseMetaData dm = con.getMetaData();
				ResultSet rs = dm.getSchemas();
				while(rs.next()){
					schemaList.add(rs.getString("TABLE_SCHEM"));
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}
		return schemaList;
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
			int dataSize =  getCountRows(ds, tableName);
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
				DataExtractionThread dataExtractionExecutor = new TeraDataDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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

}