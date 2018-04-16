package com.prifender.des.adapter.relational.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
public final class MySqlDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	
	@Value( "${des.home}")
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "MySQL";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("MySQL")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.INTEGER))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(new ConnectionParamDef().id("DatabaseName").label("DatabaseName").type(TypeEnum.STRING).required(false));

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;

		try {
			connection = getConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Mysql connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			databaseUtilService.closeSqlObject(connection);
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
				.message("Could not connect to oracle database");
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Connection connection = null;
		final String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");

		try {
			connection = getConnection(ds);
			if (connection != null) {
				metadata = metadataByConnection(connection, databaseName);
				if (metadata == null) {
					throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			databaseUtilService.closeSqlObject(connection);
		}
		return metadata;
	}

	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {

		int countRows = 0;
		Connection connection = null;
		try {
			connection = getConnection(ds);
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
				DataExtractionThread dataExtractionExecutor = new MySQLDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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

	public Connection getConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = databaseUtilService.getConnectionParam(ds, "Host");
		final String portNumber = databaseUtilService.getConnectionParam(ds, "Port");
		final String userName = databaseUtilService.getConnectionParam(ds, "UserName");
		final String password = databaseUtilService.getConnectionParam(ds, "Password");
		final String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");
		return getConnection(hostName, portNumber, userName, password, databaseName);
	}

	public Connection getConnection(String hostName, String portNumber, String userName, String password,
			String databaseName) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "com.mysql.cj.jdbc.Driver";
		String url = "jdbc:mysql://" + hostName + ":" + portNumber + ( databaseName != null ? "/" + databaseName : "");
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	public Metadata metadataByConnection(Connection con, String dataSourceName)
			throws SQLException, DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		
		dataSourceList = getAllDatasourcesFromDatabase(con);
		
		if ( dataSourceList.size() == 0) {
			throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
		}
		if (dataSourceName != null) { 
			if( dataSourceList.size() > 0 && dataSourceList.contains(dataSourceName) ) {
				dataSourceList = new ArrayList<>();
				dataSourceList.add(dataSourceName);
			} else {
				throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
			}
		}
			
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		for (String dataSource : dataSourceList) {
			List<NamedType> tableList = getDatasourceRelatedTables(con, dataSource);
			for (NamedType namedType : tableList) {
				// table entry type
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getTableRelatedColumns(con, dataSource, namedType.getName());
				entryType.setAttributes(attributeListForColumns);

				// add primary keys here
				List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
				List<Constraint> pkConstraintList = getTableRelatedPrimarykeyInfo(con, dataSource, namedType.getName());
				if (pkConstraintList != null) {
					for (Constraint constraint : pkConstraintList) {
						pkFkConstraintList.add(constraint);
					}
				}
				// add foreign keys here
				List<Constraint> fkConstraintList = getTableRelatedForeignkeyInfo(con, dataSource, namedType.getName());
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
		return metadata;
	}

	public List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException {
		List<NamedType> tableList = new ArrayList<>();
		PreparedStatement prepareStatement = null;
		ResultSet resultSet = null;
		try {
			String tablesQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE in('BASE TABLE','VIEW') AND TABLE_SCHEMA=? order by TABLE_NAME";
			prepareStatement = con.prepareStatement(tablesQuery);
			prepareStatement.setString(1, schemaName);
			resultSet = prepareStatement.executeQuery();
			while (resultSet.next()) {
				NamedType namedType = new NamedType();
				namedType.setName(resultSet.getString(1));
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
			Collections.sort(tableList, new Comparator<NamedType>() {
				public int compare(NamedType result1, NamedType result2) {
					return result1.getName().compareToIgnoreCase(result2.getName());
				}
			});
		} finally {
			databaseUtilService.closeSqlObject(resultSet, prepareStatement);
		}

		return tableList;
	}

	public List<NamedType> getTableRelatedColumns(Connection con, String schemaName, String tableName)
			throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		String columnsQuery = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			columnsQuery = "SELECT COLUMN_NAME,DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=? ";
			preparedStatement = con.prepareStatement(columnsQuery);
			preparedStatement.setString(1, tableName);
			preparedStatement.setString(2, schemaName);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(resultSet.getString(1));
				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
						.dataType(databaseUtilService.getDataType(resultSet.getString(2).toUpperCase()));
				attributeForColumn.setType(typeForCoumn);
				attributeList.add(attributeForColumn);
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return attributeList;
	}

	public List<Constraint> getTableRelatedForeignkeyInfo(Connection con, String schemaName, String tableName)
			throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getExportedKeys(schemaName, schemaName, tableName);
			while (resultSet.next()) {
				boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
				List<String> fkAttributes = new ArrayList<>();
				List<String> pkOfFkAttributes = new ArrayList<>();
				Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
				;
				if (StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME"))) {
					if (isForeignKey) {
						fkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
						pkOfFkAttributes.add(resultSet.getString("PKCOLUMN_NAME"));
						constraint.setTarget(resultSet.getString("FKTABLE_NAME"));
					}
				}
				constraint.setAttributes(pkOfFkAttributes);
				constraint.setTargetAttributes(fkAttributes);
				constraintList.add(constraint);
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return constraintList;
	}

	public List<Constraint> getTableRelatedPrimarykeyInfo(Connection con, String schemaName, String tableName)
			throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			DatabaseMetaData meta = con.getMetaData();
			resultSet = meta.getPrimaryKeys(schemaName, null, tableName);
			List<String> pkAttributes = new ArrayList<>();
			Constraint constraint = new Constraint();
			while (resultSet.next()) {
				boolean isPrimarykey = resultSet.getString("COLUMN_NAME") != null ? true : false;
				if (StringUtils.isNotBlank(resultSet.getString("COLUMN_NAME"))) {
					if (isPrimarykey) {
						pkAttributes.add(resultSet.getString("COLUMN_NAME"));
					}
				}
			}
			if (pkAttributes != null && pkAttributes.size() > 0) {
				constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
			}
			constraint.setAttributes(pkAttributes);
			constraintList.add(constraint);
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}
		return constraintList;
	}

	public List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		Statement statement = null;
		try {
			if (con != null) {
				String sql = "select schema_name from information_schema.SCHEMATA where schema_name not in('information_schema') order by schema_name";
				statement = con.createStatement();
				resultSet = statement.executeQuery(sql);
				while (resultSet.next()) {
					schemaList.add(resultSet.getString(1));
				}
			}
			Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);
		} finally {
			databaseUtilService.closeSqlObject(resultSet, statement);
		}
		return schemaList;
	}

}
