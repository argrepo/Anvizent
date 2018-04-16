package com.prifender.des.adapter.relational.sqlserver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public final class SqlServerDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;

    @Value( "${des.home}" )
    private String desHome;
    @Autowired
	EncryptionServiceClient encryptionServiceClient;
    private static final String TYPE_ID = "SqlServer";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("Microsoft SQL Server")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Password").label("Password").type(TypeEnum.PASSWORD))
			.addConnectionParamsItem(new ConnectionParamDef().id("Instance").label("Instance").type(TypeEnum.STRING).required(false))
			.addConnectionParamsItem(new ConnectionParamDef().id("DatabaseName").label("DatabaseName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("SchemaName").label("SchemaName").type(TypeEnum.STRING));
	

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
						.message("SqlServer connection successfully established.");
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
		final String schemaName = databaseUtilService.getConnectionParam(ds, "SchemaName");
		try {
			connection = getDataBaseConnection(ds);
			metadata = metadataByConnection(connection, schemaName, databaseName);
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
		final String instance = databaseUtilService.getConnectionParam(ds, "Instance");
		return getDataBaseConnection1(hostName, port, databaseName, userName, password,instance);
	}

	public Connection getDataBaseConnection1(String hostName, String port, String databaseName, String userName,
			String password,String instance) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String url = "jdbc:sqlserver://" + hostName + ":" + port + ";" + ( databaseName != null ? "databaseName=" + databaseName : "" )+";"+( instance != null ? "instance=" + instance : "" );
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		return connection;
	}

	public Metadata metadataByConnection(Connection con, String schemaName, String databaseName) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> dataSourceList = null;
		String dataSourceName = null;
		if (databaseName != null && schemaName != null ) {
			dataSourceName = "[" + databaseName + "]." + "[" + schemaName + "]";
		}
		try {
			dataSourceList = getAllDatasourcesFromDatabase(con);
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

					String dbReplaceBrackets = "[" + namedType.getName().split("\\.")[0] + "]";
					String schemeName = "[" + namedType.getName().split("\\.")[1] + "]";
					String dbName = "[" + namedType.getName().split("\\.")[2] + "]";
					String tableName = dbReplaceBrackets + "." + schemeName + "." + dbName;
					List<NamedType> attributeListForColumns = getTableRelatedColumns(con, dataSource, tableName);
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
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (DataExtractionServiceException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}
		return metadata;
	}

	public List<NamedType> getSchemaRelatedTables(Connection con, String schemaName) throws SQLException {
		List<NamedType> tableList = new ArrayList<>();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			if (con != null) {
				String tablesQuery = null;
				String[] name = schemaName.split(",");
				for (String schema : name) {
					String dbReplaceBrackets = schema.split("\\.")[0].replaceAll("\\[", "").replaceAll("\\]", "");
					String schameName = schema.split("\\.")[1].replaceAll("\\[", "").replaceAll("\\]", "");
					String dbName = schema.split("\\.")[0];
					tablesQuery = "SELECT TOP 50 QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) as TABLE_NAME_1  FROM "
							+ dbName
							+ ".INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME not in('sysdiagrams') AND TABLE_CATALOG= ? AND TABLE_SCHEMA = ? order by TABLE_NAME";
					preparedStatement = con.prepareStatement(tablesQuery);
					preparedStatement.setString(1, dbReplaceBrackets);
					preparedStatement.setString(2, schameName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						NamedType namedType = new NamedType();
						namedType.setName(
								dbName.replaceAll("\\[", "").replaceAll("\\]", "") + "." + 
						resultSet.getString(1).replaceAll("\\[", "").replaceAll("\\]", ""));
						Type type = new Type().kind(Type.KindEnum.LIST);
						namedType.setType(type);
						tableList.add(namedType);

					}
				}
				Collections.sort(tableList, new Comparator<NamedType>() {
					public int compare(NamedType result1, NamedType result2) {
						return result1.getName().compareToIgnoreCase(result2.getName());
					}
				});
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return tableList;
	}

	public List<NamedType> getTableRelatedColumns(Connection con, String schemaName, String tableName) throws SQLException, DataExtractionServiceException {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		String columnsQuery = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			if (con != null) {
				String schema = tableName.split("\\.")[0];
				columnsQuery = " SELECT QUOTENAME(COLUMN_NAME) as COLUMN_NAME, DATA_TYPE, DATA_TYPE + case when DATA_TYPE like '%char%' then '('+ cast(CHARACTER_MAXIMUM_LENGTH as varchar)+')' when DATA_TYPE in( 'numeric','real', 'decimal') then '('+ cast(NUMERIC_PRECISION as varchar)+','+ cast(NUMERIC_SCALE as varchar) +')'   else ''  end as Col_Len "
						+ " FROM  " + schema + ".INFORMATION_SCHEMA.COLUMNS WHERE  ( '" + schema
						+ "' +'.'+QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) ) = ? ";
				pstmt = con.prepareStatement(columnsQuery);
				pstmt.setString(1, tableName);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					NamedType attributeForColumn = new NamedType();
					attributeForColumn.setName(rs.getString(1).replaceAll("\\[", "").replaceAll("\\]", ""));
					Type columnInfo = getColumnInfo(con,tableName,rs.getString(1).replaceAll("\\[", "").replaceAll("\\]", ""));
					if(columnInfo != null){ 
						Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
													   .dataType(databaseUtilService.getDataType(rs.getString(2).toUpperCase())) 
													   .nullable(columnInfo.getNullable())
													   .autoIncrement(columnInfo.getAutoIncrement())
													   .size(columnInfo.getSize());
					    attributeForColumn.setType(typeForCoumn);
						attributeList.add(attributeForColumn);
					 }
				}
			}
		} finally {
			databaseUtilService.closeSqlObject(rs, pstmt);
		}

		return attributeList;
	}
	public Type getColumnInfo(Connection connection,String tableName,String columnName) throws DataExtractionServiceException{
		Type type= new Type();
		ResultSet resultSet = null;
		try {
			String dbName = tableName.split("\\.")[0].replaceAll("\\[", "").replaceAll("\\]", "");
			String schema = tableName.split("\\.")[1].replaceAll("\\[", "").replaceAll("\\]", "");
			String tabName = tableName.split("\\.")[2].replaceAll("\\[", "").replaceAll("\\]", "");
			DatabaseMetaData meta = connection.getMetaData();
			resultSet = meta.getColumns(dbName, schema, tabName,columnName);
			if (resultSet.next()) {
				type.setSize(resultSet.getInt("COLUMN_SIZE"));
				type.setAutoIncrement(resultSet.getString("IS_AUTOINCREMENT").equals("YES") ? true : false);
				type.setNullable(resultSet.getString("IS_NULLABLE").equals("YES") ? true : false); 
			} 
		} catch (SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}finally{
			databaseUtilService.closeSqlObject(resultSet);
		}
		return type;
	}
	
	public List<Constraint> getTableRelatedFkInfo(Connection con, String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			if (con != null) {
				String dbName = tableName.split("\\.")[0];
				String schema = tableName.split("\\.")[1].replaceAll("\\[", "").replaceAll("\\]", "");
				String tabName = tableName.split("\\.")[2].replaceAll("\\[", "").replaceAll("\\]", "");

				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder
						.append("SELECT SCHEMA_NAME(t.schema_id) AS type_schema,object_name(c.object_id)as Table_Name,  c.name AS column_name ,c.column_id  ,c.max_length ")
						.append(" ,c.precision  ,c.scale ,i.name AS index_name ,is_identity  ,i.is_primary_key  ,f.name AS foreign_key_name  ,OBJECT_NAME (f.referenced_object_id) AS referenced_object ,COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name FROM ")
						.append(dbName)
						.append(".sys.columns AS c INNER JOIN ").append(dbName).append(".sys.types AS t  ON c.user_type_id=t.user_type_id LEFT OUTER JOIN ")
						.append(dbName).append(".sys.index_columns AS ic ")
						.append(" ON ic.object_id = c.object_id   AND c.column_id = ic.column_id LEFT OUTER JOIN ")
						.append(dbName)
						.append(".sys.indexes AS i ON i.object_id = ic.object_id AND i.index_id = ic.index_id ")
						.append(" LEFT OUTER JOIN ").append(dbName)
						.append(".sys.foreign_key_columns AS fc  ON fc.parent_object_id = c.object_id  AND COL_NAME(fc.parent_object_id, fc.parent_column_id) = c.name ")
						.append(" LEFT OUTER JOIN ").append(dbName)
						.append(".sys.foreign_keys AS f  ON f.parent_object_id = c.object_id  AND fc.constraint_object_id = f.object_id ")
						.append(" WHERE SCHEMA_NAME(t.schema_id)=? and c.object_id = OBJECT_ID('").append(tabName)
						.append("') ORDER BY c.column_id ");

				preparedStatement = con.prepareStatement(queryBuilder.toString());
				preparedStatement.setString(1, schema);
				resultSet = preparedStatement.executeQuery();
				while (resultSet.next()) {
					if (resultSet.getString("referenced_column_name") != null) {
						boolean isForeignKey = resultSet.getString("referenced_column_name") != null ? true : false;
						List<String> fkAttributes = new ArrayList<>();
						List<String> pkOfFkAttributes = new ArrayList<>();
						Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
						if (StringUtils.isNotBlank(resultSet.getString("referenced_column_name"))) {
							if (isForeignKey) {
								fkAttributes.add(resultSet.getString("referenced_column_name"));
								pkOfFkAttributes.add(resultSet.getString("column_name"));
								constraint.setTarget(dbName+"."+resultSet.getString("type_schema")+"."+resultSet.getString("referenced_object"));
							}
						}
						constraint.setAttributes(pkOfFkAttributes);
						constraint.setTargetAttributes(fkAttributes);
						constraintList.add(constraint);
					}
				}
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return constraintList;
	}

	public List<Constraint> getTableRelatedPkInfo(Connection con, String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			if (con != null) {
				String[] dbSchemaTable = StringUtils.split(tableName, ".");
				String dbName = dbSchemaTable[0];
				String schema = dbSchemaTable[1];
				String tabName = dbSchemaTable[2];

				StringBuilder queryBuilder = new StringBuilder();
				queryBuilder.append("SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN ")
						.append("FROM "+dbName+".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC ")
						.append("INNER JOIN "+dbName+".INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ")
						.append("ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND ")
						.append("TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND  ")
						.append("KU.table_name=? and KU.CONSTRAINT_SCHEMA=? ")
						.append("ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;");

				preparedStatement = con.prepareStatement(queryBuilder.toString());
				preparedStatement.setString(1, tabName);
				preparedStatement.setString(2, schema);
				resultSet = preparedStatement.executeQuery();
				List<String> pkAttributes = new ArrayList<>();
				Constraint constraint = new Constraint();
				while (resultSet.next()) {
						pkAttributes.add(resultSet.getString("PRIMARYKEYCOLUMN"));
				}
				if (pkAttributes != null && pkAttributes.size() > 0) {
					constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
					constraint.setAttributes(pkAttributes);
					constraintList.add(constraint);
				}
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}
		return constraintList;
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
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			
			String schemaName = databaseUtilService.getConnectionParam(ds, "SchemaName");
			String[] schemaTable = StringUtils.split(tableName, ".");
			if (schemaTable.length == 3 ) {
				schemaName = schemaTable[1];
				tableName = schemaTable[2];
			}
			if (schemaTable.length == 2 ) {
				schemaName = schemaTable[0];
				tableName = schemaTable[1];
			}
			countRows = databaseUtilService.getCountRows(connection, schemaName + "." + tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} catch (Exception e) {
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
			String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
			try {
				DataExtractionThread dataExtractionExecutor = new SqlServerDataExtractionExecutor(ds, spec, job, dataSize, adapterHome,messaging,encryptionServiceClient);
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