package com.prifender.des.util;

import java.io.File;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import javax.management.MalformedObjectNameException;
import org.apache.commons.lang3.StringUtils;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Type;
import com.prifender.des.model.Type.DataTypeEnum;

public final class DatabaseUtil {

	/**
	 * To close Connection object
	 * @param connection
	 */
	public static void closeSqlObject(Connection connection) {
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
	public static void closeSqlObject(Statement statement) {
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
	public static void closeSqlObject(ResultSet resultSet) {
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
	public static void closeSqlObject(ResultSet resultSet, Statement statement) {
		closeSqlObject(resultSet);
		closeSqlObject(statement);
	}
	
	/**
	 * To fetch the count of the table in selected schema
	 * @param con
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public static int getCountRows(Connection con,String tableName) throws SQLException {
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
	public static DataTypeEnum getDataType(String dataType) {
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
	
	
	/**
	 * @param metadata
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public static boolean isValidColumn(Metadata metadata, final String tableName, final String columnName) {
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
	
	/**
	 * @param metadata
	 * @param tableName
	 * @return
	 */
	public static boolean isValidTable(Metadata metadata, final String tableName) {
		for (final NamedType table : metadata.getObjects()) {
			if (table.getName().equals(tableName)) {
				return true;
			}
		}
		return false;
	}
	
 public static class Temp {
		
		private Temp() 
		{
		}

		public static final String TEMP_FILE_DIR;
		
		static 
		{
			TEMP_FILE_DIR = System.getProperty("java.io.tmpdir") + "/Prifender_Des/";
			
			File tempPath = new File(TEMP_FILE_DIR);
			
			try {
				
				if (!tempPath.exists()) 
				{
					tempPath.mkdirs();
				}
			} 
			catch (Exception e) {
				
				System.out.println("Unable to create temp folder : " + TEMP_FILE_DIR + "; " + e.getMessage());
				
			}
		}

		public static String getTempFileDir() {
			return TEMP_FILE_DIR;
		}
	}
	
	/**
	 * @param baseDir
	 * @param dirName
	 * @return
	 */
	public static String createDir(String baseDir, String dirName) {
		if (StringUtils.isBlank(baseDir)) {
			baseDir = Temp.getTempFileDir();
		}
		if (StringUtils.isNotBlank(dirName)) {
			dirName = baseDir + "/" + dirName + "/";
			if (!new File(dirName).exists()) {
				new File(dirName).mkdirs();
			}
		}
		return dirName;
	}

	/**
	 * @param date
	 * @return
	 */
	public static String getConvertedDate(Date date) 
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
		
		return formatter.format(date);
	}

	/**
	 * @param sampleSize
	 * @param noOfContainers
	 * @return
	 */
	public static int generateTaskSampleSize(int sampleSize , int noOfContainers) 
	{
		return (sampleSize/noOfContainers);	
		
	}

	/**
	 * @param str
	 * @return
	 */
	public static String removeLastChar(String str) 
	{
		
		return str.substring(0, str.length() - 1);
		
	}

	/**
	 * @return
	 * @throws MalformedObjectNameException
	 * @throws NullPointerException
	 * @throws UnknownHostException
	 */
	public static String getUUID() throws MalformedObjectNameException, NullPointerException, UnknownHostException {
		
		UUID uuid = UUID.randomUUID();
		
		String randomUUIDString = uuid.toString();
		
		return  randomUUIDString;
	}

/**
 * @param ds
 * @param spec
 * @param pattern
 * @return
 */
public static String getDataSourceColumnNames(DataSource ds, DataExtractionSpec spec,String pattern){
		
		StringJoiner dataSourceColumnNames = new StringJoiner(","); 
		
		for (final DataExtractionAttribute attribute : spec.getAttributes()) 
		{
			List<DataExtractionAttribute> childAttributeList = attribute.getChildren();
			
			if (childAttributeList != null && childAttributeList.size() > 0) 
			{
				for (DataExtractionAttribute childAttribute : childAttributeList) 
				{
					dataSourceColumnNames.add(attribute.getName() + pattern + childAttribute.getName());
				}
			} 
			else 
			{
				dataSourceColumnNames.add(attribute.getName());
			}
		}
		return dataSourceColumnNames.toString();
	}
}
