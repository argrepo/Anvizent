package com.prifender.des.adapter.relational.oracle;

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
public final class OracleDataSourceAdapter implements DataSourceAdapter
{
	@Autowired
	DatabaseUtil databaseUtilService;
	
    @Value( "${des.home}" )
    private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
    private static final String TYPE_ID = "Oracle";
    
    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID )
        .label( "Oracle" )
        .addConnectionParamsItem( new ConnectionParamDef().id( "Host" ).label( "Host" ).type( TypeEnum.STRING ) )
        .addConnectionParamsItem( new ConnectionParamDef().id( "Port" ).label( "Port" ).type( TypeEnum.STRING ) )
        .addConnectionParamsItem( new ConnectionParamDef().id( "SID" ).label( "SID" ).type( TypeEnum.STRING ) )
        .addConnectionParamsItem( new ConnectionParamDef().id( "UserName" ).label( "UserName" ).type( TypeEnum.STRING ) )
        .addConnectionParamsItem( new ConnectionParamDef().id( "Password" ).label( "Password" ).type( TypeEnum.PASSWORD ) )
        .addConnectionParamsItem( new ConnectionParamDef().id( "DatabaseName" ).label( "DatabaseName" ).type( TypeEnum.STRING ).required(false));

    @Override
    public DataSourceType getDataSourceType()
    {
        return TYPE;
    }

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;

		try {
			connection = getConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("oracle connection successfully established.");
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
			metadata = metadataByConnection(connection, databaseName);
			if (metadata == null) {
				throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			databaseUtilService.closeSqlObject(connection);
		}
		return metadata;
	}

	public Connection getConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = databaseUtilService.getConnectionParam(ds, "Host");
		final String portNumber = databaseUtilService.getConnectionParam(ds, "Port");
		final String Sid = databaseUtilService.getConnectionParam(ds, "SID");
		final String userName = databaseUtilService.getConnectionParam(ds, "UserName");
		final String password = databaseUtilService.getConnectionParam(ds, "Password");
		String databaseName = databaseUtilService.getConnectionParam(ds, "DatabaseName");
		return getConnection(hostName, portNumber, Sid, userName, password, databaseName);
	}

	public Connection getConnection(String hostName, String portNumber, String Sid, String userName,
			String password, String databaseName) throws SQLException, ClassNotFoundException {
		Connection connection = null;
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@" + hostName + ":" + portNumber + ":" + Sid;
		Class.forName(driver);
		connection = DriverManager.getConnection(url, userName, password);
		if (databaseName != null) { 
			connection.setSchema(databaseName);
		}
		return connection;
	}
	public Metadata metadataByConnection(Connection con,String schemaName) throws DataExtractionServiceException, SQLException {
		Metadata metadata = new Metadata();
		List<String> databaseList = null;
		databaseList = getAllDatasourcesFromDatabase(con);
			
			if ( databaseList.size() == 0) {
				throw new DataExtractionServiceException(new Problem().code("No Databases").message("No Databases found in "+TYPE_ID +"database."));
			}
			
			if (schemaName != null) {
				if (databaseList.contains(schemaName)) {
					databaseList = new ArrayList<String>();
					databaseList.add(schemaName);
				} else {
					throw new DataExtractionServiceException(new Problem().code("Unknown database").message("Database not found in "+TYPE_ID +"database."));
				}
			}
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for(String dataSource : databaseList){
				List<NamedType> tableList =	getDatasourceRelatedTables(con,dataSource);
				for(NamedType namedType : tableList ){
					//table entry type
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getTableRelatedColumns(con,dataSource,namedType.getName());
					entryType.setAttributes(attributeListForColumns);
					
					//add primary keys here
					List<Constraint> pkFkConstraintList = new ArrayList<Constraint>();
					List<Constraint> pkConstraintList =	getTableRelatedPkInfo(con,dataSource,namedType.getName());
					if(pkConstraintList != null){
						
						 for(Constraint constraint : pkConstraintList){ 
							 pkFkConstraintList.add(constraint);
						 } 
					}
					//add foreign keys here
					List<Constraint> fkConstraintList =	getTableRelatedFkInfo(con,dataSource,namedType.getName());
					if(fkConstraintList != null){
						 for(Constraint constraint : fkConstraintList){ 
							 pkFkConstraintList.add(constraint);
						 } 
					}
					namedType.setName(schemaName + "." + namedType.getName());
					entryType.setConstraints(pkFkConstraintList);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
			}
		return metadata;
	}
	public List<NamedType> getDatasourceRelatedTables(Connection con, String schemaName) throws SQLException {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<NamedType> tableList = new ArrayList<>();
		try {
					String tablesQuery = null;
						tablesQuery = " select TABLE_NAME from SYS.ALL_TABLES  where OWNER = ? union all select VIEW_NAME from SYS.ALL_VIEWS where OWNER =  ? ";
						preparedStatement = con.prepareStatement(tablesQuery);
						preparedStatement.setString(1, schemaName);
						preparedStatement.setString(2, schemaName);
						resultSet = preparedStatement.executeQuery();
						while (resultSet.next()) {
							NamedType namedType = new  NamedType();
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
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}

		return tableList;
	}

	public List<NamedType> getTableRelatedColumns(Connection con,String schemaName, String tableName) throws SQLException, DataExtractionServiceException {
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		String columnsQuery = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			if (con != null) {
					columnsQuery = "SELECT column_name, data_type , DATA_LENGTH  FROM all_tab_columns where table_name = ? AND OWNER= ? ";
					preparedStatement = con.prepareStatement(columnsQuery);
					preparedStatement.setString(1, tableName);
					preparedStatement.setString(2, schemaName);
					resultSet = preparedStatement.executeQuery();
					while (resultSet.next()) {
						NamedType attributeForColumn = new  NamedType();
						attributeForColumn.setName(resultSet.getString(1));
						Type columnInfo = getColumnInfo(con,schemaName,tableName,resultSet.getString(1));
						if(columnInfo != null){ 
							Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE)
														   .dataType(databaseUtilService.getDataType(resultSet.getString(2).toUpperCase())) 
														   .nullable(columnInfo.getNullable())
														   .autoIncrement(columnInfo.getAutoIncrement())
														   .size(columnInfo.getSize());
						    attributeForColumn.setType(typeForCoumn);
							attributeList.add(attributeForColumn);
						 }
					}
				} 
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
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
	public List<Constraint> getTableRelatedFkInfo(Connection con,String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		List<Constraint> constraintList = new ArrayList<>();
		try {
			if (con != null) {
				DatabaseMetaData meta = con.getMetaData();
					resultSet = meta.getExportedKeys(null, schemaName, tableName);
					while (resultSet.next()) { 
						 boolean isForeignKey = resultSet.getString("FKCOLUMN_NAME") != null ? true : false;
						 List<String> fkAttributes = new ArrayList<>();
						 List<String> pkOfFkAttributes = new ArrayList<>();
						  Constraint constraint = new Constraint().kind(Constraint.KindEnum.FOREIGN_KEY);
						  if(StringUtils.isNotBlank(resultSet.getString("FKCOLUMN_NAME"))){
							  if(isForeignKey){
								  fkAttributes.add(resultSet.getString("FKCOLUMN_NAME"));
								  pkOfFkAttributes.add(resultSet.getString("PKCOLUMN_NAME"));
								  constraint.setTarget(resultSet.getString("FKTABLE_SCHEM")+"."+resultSet.getString("FKTABLE_NAME"));
							  }
						  }
						  constraint.setAttributes(pkOfFkAttributes);
						  constraint.setTargetAttributes(fkAttributes);
						  constraintList.add(constraint);
					}
			}
		} finally {
			databaseUtilService.closeSqlObject(resultSet);
		}

		return constraintList;
	}
	public List<Constraint> getTableRelatedPkInfo(Connection con,String schemaName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		PreparedStatement preparesStatement = null;
		List<Constraint> constraintList = new ArrayList<Constraint>();
		try {
			if (con != null) {
				   DatabaseMetaData meta = con.getMetaData();
				 // ORACLE_DB_TYPE
					resultSet = meta.getPrimaryKeys(schemaName, null, tableName);
						 List<String> pkAttributes = new ArrayList<>();
						 Constraint constraint = new Constraint();
						while (resultSet.next()) {
							boolean isPrimarykey = resultSet.getString("COLUMN_NAME") != null ? true : false;
							  if(StringUtils.isNotBlank(resultSet.getString("COLUMN_NAME"))){
								  if(isPrimarykey){
									  pkAttributes.add(resultSet.getString("COLUMN_NAME"));
								  }
							  }
							}
						if(pkAttributes != null && pkAttributes.size() > 0){
							 constraint.kind(Constraint.KindEnum.PRIMARY_KEY);
						 }
						 constraint.setAttributes(pkAttributes);
						 constraintList.add(constraint);
					}
		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparesStatement);
		}
		return constraintList;
	}
	public List<String> getAllDatasourcesFromDatabase(Connection con) throws SQLException {
		List<String> schemaList = new ArrayList<>();
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;
		try {
			if (con != null) {
				DatabaseMetaData meta = con.getMetaData();
				resultSet = meta.getSchemas();
				while (resultSet.next()) {
					String schema = resultSet.getString("TABLE_SCHEM");
					if (isWantedSchema(schema)) {
						schemaList.add(schema);
					}
				  }
				}
				Collections.sort(schemaList, String.CASE_INSENSITIVE_ORDER);

		} finally {
			databaseUtilService.closeSqlObject(resultSet, preparedStatement);
		}
		return schemaList;
	}
	  
	public boolean isWantedSchema(String schemaName) {
		String[] defaultSchemas = {};
		List<String> defaultSchemasList = new ArrayList<>();
			defaultSchemas = new String[] { "ANONYMOUS"/*, "APEX_030200", "APEX_PUBLIC_USER", "APPQOSSYS", "BI", "CTXSYS",
					"DBSNMP", "DIP", "EXFSYS", "FLOWS_FILES", "HR", "IX", "MDDATA", "MDSYS", "MGMT_VIEW", "OE",
					"OLAPSYS", "ORACLE_OCM", "ORDDATA", "ORDPLUGINS", "ORDSYS", "OUTLN", "OWBSYS", "OWBSYS_AUDIT", "PM",
					"SCOTT", "SH", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "SYS",
					"SYSMAN", "SYSTEM", "WMSYS", "XDB", "XS$NULL"*/ }; 
		boolean wantedSchema = true;
		for (int i = 0; i < defaultSchemas.length; i++) {
			defaultSchemasList.add(defaultSchemas[i]);
		}
		if (defaultSchemasList.contains(schemaName)) {
			wantedSchema = false;
		}
		return wantedSchema;
	}
	
	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getConnection(ds);
			if (connection != null) {
				countRows = databaseUtilService.getCountRows(connection, tableName);
			}
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
			int countRows = getCountRows(ds, tableName);
			if (countRows == 0) {
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
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new OracleDataExtractionExecutor(ds, spec, job, countRows, adapterHome,messaging,encryptionServiceClient);
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
