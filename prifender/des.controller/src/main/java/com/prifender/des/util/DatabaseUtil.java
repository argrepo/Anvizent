package com.prifender.des.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.stereotype.Component;

import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Type;
import com.prifender.des.model.Type.DataTypeEnum;

@Component
public class DatabaseUtil {

	/**
	 * To close Connection object
	 * @param connection
	 */
	public void closeSqlObject(Connection connection) {
		try {
			if (connection != null && !connection.isClosed())
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * To close Statement object
	 * @param statement
	 */
	public void closeSqlObject(Statement statement) {
		try {
			if (statement != null && !statement.isClosed())
				statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * To close ResultSet object
	 * @param resultSet
	 */
	public void closeSqlObject(ResultSet resultSet) {
		try {
			if (resultSet != null && !resultSet.isClosed())
				resultSet.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * To close ResultSet and Statement objects
	 * 
	 * @param resultSet
	 * @param statement
	 */
	public void closeSqlObject(ResultSet resultSet, Statement statement) {
		closeSqlObject(resultSet);
		closeSqlObject(statement);
	}
	
	/**
	 * To  get the required connection parameter
	 * @param ds
	 * @param param
	 * @return
	 */
	public final String getConnectionParam(final DataSource ds, final String param) {
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
	
	/**
	 * To fetch the count of the table in selected schema
	 * @param con
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public int getCountRows(Connection con,String tableName) throws SQLException {
		int countRows = 0;
		ResultSet resultSet = null;
		Statement statement = null;
		try {
			String sql = "SELECT COUNT(1) FROM " + tableName;
			statement = con.createStatement();
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				countRows = resultSet.getInt(1);
			}
		} finally {
			closeSqlObject(resultSet, statement);
		}
		return countRows;
	}
	

	/**
	 * To select the appropriate DataTypeEnum object
	 * @param dataType
	 * @return
	 */
	public DataTypeEnum getDataType(String dataType) {
		DataTypeEnum dataTypeEnum = null;
		if (dataType.equals("VARCHAR") || dataType.equals("TEXT") || dataType.equals("PICKLIST")
				|| dataType.equals("VARCHAR2") || dataType.equals("NVARCHAR") || dataType.equals("NCHAR")
				|| dataType.equals("REFERENCE") || dataType == "TEXTAREA" || dataType == "PHONE"
				|| dataType.equals("URL") || dataType.equals("LONGVARCHAR") || dataType.equals("LONGTEXTAREA")
				|| dataType.equals("ID") || dataType.equals("NTEXT") || dataType.equals("UNIQUEIDENTIFIER")
				|| dataType.equals("CHAR") || dataType.equals("CHARACTER") || dataType.equals("CHARACTER VARYING")
				|| dataType.equals("BIT") || dataType.equals("VARBIT") || dataType.equals("BIT VARYING") || dataType.equals("STRING") || dataType.equals("String") ) {
			dataTypeEnum = Type.DataTypeEnum.STRING;
		} else if (dataType.equals("VARBINARY") || dataType.equals("LONGVARBINARY")) {
			dataTypeEnum = Type.DataTypeEnum.BINARY;
		} else if (dataType.equals("BOOLEAN") || dataType.equals("BOOL")) {
			dataTypeEnum = Type.DataTypeEnum.BOOLEAN;
		} else if (dataType.equals("INT") || dataType.equals("NUMBER") || dataType.equals("BIGINT")
				|| dataType.equals("INTEGER") || dataType.equals("MEDIUMINT") || dataType.equals("SMALLINT")
				|| dataType.equals("TINYINT") || dataType.equals("INT UNSIGNED") || dataType.equals("SMALLSERIAL")
				|| dataType.equals("SERIAL") || dataType.equals("BIGSERIAL") || dataType.equals("INT32")) {
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		} else if (dataType.equals("MONEY") || dataType.equals("CURRENCY")) {
			dataTypeEnum = Type.DataTypeEnum.CURRENCY;
		} else if (dataType.equals("DECIMAL") || dataType.equals("FIXED_LEN_BYTE_ARRAY")) {
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		} else if (dataType.equals("FLOAT") || dataType.equals("DOUBLE") || dataType.equals("NUMERIC")
				|| dataType.equals("LONG") || dataType.equals("REAL")) {
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		} else if (dataType.equals("DATETIME") || dataType.equals("DATE") || dataType.equals("TIMESTAMP")
				|| dataType.equals("DATETIME2") || dataType == "DATETIMEOFFSET" || dataType.equals("SMALLDATETIME")
				|| dataType.equals("TIMESTAMP WITHOUT TIME ZONE") || dataType.equals("TIMESTAMP WITH TIME ZONE")
				|| dataType.equals("YEAR")) {
			dataTypeEnum = Type.DataTypeEnum.DATE;
		} else if (dataType.equals("TIME") || dataType.equals("TIME WITHOUT TIME ZONE")
				|| dataType.equals("TIME WITH TIME ZONE")) {
			dataTypeEnum = Type.DataTypeEnum.TIME;
		} else if (dataType.equals("BINARY")) {
			dataTypeEnum = Type.DataTypeEnum.BINARY;
		}
		return dataTypeEnum;
	}
	
	
	public boolean isValidColumn(Metadata metadata, final String tableName, final String columnName) {
		for (final NamedType table : metadata.getObjects()) {
			if (table.getName().equals(tableName)) {
				final Type entryType = table.getType().getEntryType();

				for (final NamedType column : entryType.getAttributes()) {
					if (column.getName().equals(columnName)) {
						return true;
					}
				}
			}
		}

		return false;
	}
	
	public boolean isValidTable(Metadata metadata, final String tableName) {
		for (final NamedType table : metadata.getObjects()) {
			if (table.getName().equals(tableName)) {
				return true;
			}
		}
		return false;
	}

	public String removeLastChar(String str) {
		return str.substring(0, str.length() - 1);
	}
	

}
