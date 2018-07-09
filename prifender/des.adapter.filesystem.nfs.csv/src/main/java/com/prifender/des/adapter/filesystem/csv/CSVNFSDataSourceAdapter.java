package com.prifender.des.adapter.filesystem.csv;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.oro.text.GlobCompiler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionContext;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;
import com.sun.xfile.XFile;
import com.sun.xfile.XFileInputStream;

/**
 * The CSVNFSDataSourceAdapter Component is a file system , implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-04-06
 */

@Component
public final class CSVNFSDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	@Value("${scheduling.taskStatusQueue}")
	private String taskStatusQueueName;

	private static final String JOB_NAME = "local_project.prifender_csv_nfs_v3_0_1.Prifender_CSV_NFS_v3";

	private static final String DEPENDENCY_JAR = "prifender_csv_nfs_v3_0_1.jar";

	private static final String JOB_NAME_WithoutHeader = "local_project.prifender_csv_nfs_without_header_v3_0_1.Prifender_CSV_NFS_Without_Header_v3";

	private static final String DEPENDENCY_JAR_WithoutHeader = "prifender_csv_nfs_without_header_v3_0_1.jar";

	public static final String TYPE_ID = "CSVNFS";
	public static final String TYPE_LABEL = "CSV NFS";

	// File

	public static final String PARAM_FILE_ID = "File";
	public static final String PARAM_FILE_LABEL = "File";
	public static final String PARAM_FILE_DESCRIPTION = "The path of the file";

	public static final ConnectionParamDef PARAM_FILE = new ConnectionParamDef().id(PARAM_FILE_ID).label(PARAM_FILE_LABEL).description(PARAM_FILE_DESCRIPTION).type(TypeEnum.STRING);

	// ShareName

	public static final String PARAM_SHARE_ID = "Share";
	public static final String PARAM_SHARE_LABEL = "Share";
	public static final String PARAM_SHARE_DESCRIPTION = "The File Share Name";

	public static final ConnectionParamDef PARAM_SHARE = new ConnectionParamDef().id(PARAM_SHARE_ID).label(PARAM_SHARE_LABEL).description(PARAM_SHARE_DESCRIPTION).type(TypeEnum.STRING).required(false);

	// Delimiter

	public static final String PARAM_DELIMITER_ID = "Delimiter";
	public static final String PARAM_DELIMITER_LABEL = "Delimiter";
	public static final String PARAM_DELIMITER_DESCRIPTION = "The delimeter of the file";

	public static final ConnectionParamDef PARAM_DELIMITER = new ConnectionParamDef().id(PARAM_DELIMITER_ID).label(PARAM_DELIMITER_LABEL).description(PARAM_DELIMITER_DESCRIPTION).type(TypeEnum.STRING).required(false);

	// File Regular Expression

	public static final String PARAM_FILE_REG_EXP_ID = "FileRegExp";
	public static final String PARAM_FILE_REG_EXP_LABEL = "File Regular Expression";
	public static final String PARAM_FILE_REG_EXP_DESCRIPTION = "The file regular expression";

	public static final ConnectionParamDef PARAM_FILE_REG_EXP = new ConnectionParamDef().id(PARAM_FILE_REG_EXP_ID).label(PARAM_FILE_REG_EXP_LABEL).description(PARAM_FILE_REG_EXP_DESCRIPTION).type(TypeEnum.STRING).required(false);

	// Header Exists

	public static final String PARAM_HEADER_EXISTS_ID = "HeaderExists";
	public static final String PARAM_HEADER_EXISTS_LABEL = "Header Exists";
	public static final String PARAM_HEADER_EXISTS_DESCRIPTION = "The headers are exists in file";

	public static final ConnectionParamDef PARAM_HEADER_EXISTS = new ConnectionParamDef().id(PARAM_HEADER_EXISTS_ID).label(PARAM_HEADER_EXISTS_LABEL).description(PARAM_HEADER_EXISTS_DESCRIPTION).type(TypeEnum.BOOLEAN);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_FILE).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(PARAM_SHARE).addConnectionParamsItem(PARAM_DELIMITER).addConnectionParamsItem(PARAM_FILE_REG_EXP)
			.addConnectionParamsItem(PARAM_HEADER_EXISTS);

	String _delimiter = "";

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		XFile xFile = null;
		List<String> listFiles = null;
		try
		{
			final String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
			final String host = getConnectionParam(ds, PARAM_HOST_ID);
			final String ShareName = getConnectionParam(ds, PARAM_SHARE_ID);
			xFile = getNfsFileSystem(ds);
			if( xFile != null && xFile.exists() )
			{
				listFiles = getNfsFilesList(xFile, pattern, host, ShareName, fileName);
				if( listFiles != null && listFiles.size() > 0 )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("CSV NFS File System connection successfully established.");
				}
				else
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("File's not found in a given file regular expression '" + pattern + "' or file path.");
				}
			}
		}
		catch ( Exception e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to CSV NFS File System.");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{

		final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
		final String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String ShareName = getConnectionParam(ds, PARAM_SHARE_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));

		return getMetadata(fileName, pattern, delimiter, csvHeaderExists, host, ShareName);

	}

	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int objectCount = 0;
		Map<String, Integer> csvFilesMap = null;
		try
		{
			String tableName = spec.getCollection();
			final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
			if( StringUtils.isNotBlank(delimiter) )
			{
				_delimiter = delimiter;
			}
			else
			{
				getDelimiter(ds, tableName);
			}

			if( spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				objectCount = (int) getFilesSize(ds, tableName);
			}
			else
			{
				csvFilesMap = getFilesList(ds, tableName);

				for (Map.Entry<String, Integer> entry : csvFilesMap.entrySet())
				{
					objectCount += entry.getValue();
				}
			}
		}
		catch ( FileNotFoundException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		return objectCount;
	}

	/**
	 * @param xfile
	 * @param pattern
	 * @param host
	 * @param shareName
	 * @param fileName
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private List<String> getNfsFilesList(XFile xfile, String pattern, String host, String shareName, String fileName) throws FileNotFoundException, IOException
	{
		String[] listFiles = null;
		List<String> filesList = new ArrayList<>();
		if( xfile != null )
		{
			if( xfile.exists() && xfile.isDirectory() )
			{
				if( StringUtils.isNotBlank(pattern) )
				{
					listFiles = listNfsFiles(xfile, pattern);
				}
				else
				{
					listFiles = xfile.list();
				}
				for (String file : listFiles)
				{
					filesList.add(host + shareName + fileName + "/" + file);
				}
			}
			else
			{
				filesList.add(host + shareName + fileName);
			}
		}
		return filesList;

	}

	/**
	 * @param xfile
	 * @param pattern
	 * @return
	 * @throws IOException
	 */
	private String[] listNfsFiles(XFile xfile, String pattern) throws IOException
	{
		String[] listFiles = xfile.list();
		List<String> files = new ArrayList<String>();
		Pattern patt = Pattern.compile(GlobCompiler.globToPerl5(pattern.toCharArray(), GlobCompiler.DEFAULT_MASK));
		for (String file : listFiles)
		{
			Matcher mat = patt.matcher(file);
			if( mat.matches() )
			{
				files.add(file);
			}
		}
		return (String[]) files.toArray(new String[files.size()]);
	}

	/**
	 * @param fileName
	 * @param pattern
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param host
	 * @param shareName
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private Metadata getMetadata(String fileName, String pattern, String delimiter, boolean csvHeaderExists, String host, String shareName) throws DataExtractionServiceException
	{
		List<String> filesList = null;
		XFile xFile = null;
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		try
		{
			if( StringUtils.isNotBlank(host) && StringUtils.isNotBlank(shareName) )
			{
				xFile = getNfsFile(host, shareName, fileName);
				if( xFile != null && xFile.exists() )
				{
					filesList = getNfsFilesList(xFile, pattern, host, shareName, fileName);

					if( filesList != null && filesList.size() > 0 )
					{
						for (String fName : filesList)
						{

							NamedType namedType = getFileNameFromList(fileName);
							Type entryType = new Type().kind(Type.KindEnum.OBJECT);
							namedType.getType().setEntryType(entryType);
							List<NamedType> attributeListForColumns = getHeadersFromFile(fName, delimiter, csvHeaderExists);

							entryType.setAttributes(attributeListForColumns);
							namedTypeObjectsList.add(namedType);
							metadata.setObjects(namedTypeObjectsList);

							if( xFile.isDirectory() )
							{
								break;
							}
							else
							{
								continue;
							}
						}
					}
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + fileName + "'"));
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));

		}
		return metadata;
	}

	/**
	 * @param ds
	 * @param tableName
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private long getFilesSize(DataSource ds, String tableName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{

		final String regex = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
		if( shareName.startsWith("/") && shareName.endsWith("/") )
		{
			return getCsvFilesSize(tableName, regex, host, shareName);
		}
		else
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message("share '" + shareName + "' path should start and end with '/'"));
		}
	}

	/**
	 * @param csvFileName
	 * @param regex
	 * @param host
	 * @param shareName
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private long getCsvFilesSize(String csvFileName, String regex, String host, String shareName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		int filesSize = 0;
		XFile xfile = null;
		List<String> csvFilesList = null;
		try
		{
			if( StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(host) )
			{
				xfile = getNfsFile(host, shareName, csvFileName);
				if( xfile != null && xfile.exists() )
				{
					csvFilesList = getNfsFilesList(xfile, regex, host, shareName, csvFileName);
					if( csvFilesList != null && csvFilesList.size() > 0 )
					{
						for (String fileName : csvFilesList)
						{
							filesSize += getFileSize(fileName);
						}
					}
					else
					{
						throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + csvFileName + "'"));
					}
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + csvFileName + "'"));
				}
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFileSize").message(e.getMessage()));
		}

		return filesSize;
	}

	/**
	 * @param ds
	 * @param tableName
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void getDelimiter(DataSource ds, String tableName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{

		final String regex = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);

		if( shareName.startsWith("/") && shareName.endsWith("/") )
		{
			getCsvDelimiter(tableName, regex, delimiter, csvHeaderExists, host, shareName);
		}
		else
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message("share '" + shareName + "' path should start and end with '/'"));
		}
	}

	/**
	 * @param csvFileName
	 * @param pattern
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param host
	 * @param shareName
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void getCsvDelimiter(String csvFileName, String pattern, String delimiter, boolean csvHeaderExists, String host, String shareName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		XFile xFile = null;
		List<String> filesList = null;
		try
		{
			if( StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(host) )
			{
				xFile = getNfsFile(host, shareName, csvFileName);
				if( xFile != null && xFile.exists() )
				{
					filesList = getNfsFilesList(xFile, pattern, host, shareName, csvFileName);
					if( filesList != null && filesList.size() > 0 )
					{
						for (String fileName : filesList)
						{
							getCsvFormat(getDelimiter(fileName, delimiter), csvHeaderExists);
							break;
						}
					}
					else
					{
						throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + csvFileName + "'"));
					}
				}
				else
				{
					throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + csvFileName + "'"));
				}
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}

	}

	/**
	 * @param ds
	 * @param tableName
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Map<String, Integer> getFilesList(DataSource ds, String tableName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{

		final String regex = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
		return getCsvFilesList(tableName, regex, delimiter, host, shareName);
	}

	/**
	 * @param csvFileName
	 * @param pattern
	 * @param delimiter
	 * @param host
	 * @param shareName
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Map<String, Integer> getCsvFilesList(String csvFileName, String pattern, String delimiter, String host, String shareName) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		XFile xFile = null;
		List<String> filesList = null;
		Map<String, Integer> filesMap = new HashMap<String, Integer>();
		try
		{
			if( StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(host) )
			{
				xFile = getNfsFile(host, shareName, csvFileName);

				if( xFile != null && xFile.exists() )
				{
					filesList = getNfsFilesList(xFile, pattern, host, shareName, csvFileName);

					if( filesList != null && filesList.size() > 0 )
					{
						for (String fileName : filesList)
						{

							filesMap.put(fileName, (int) getFileSize(fileName));
						}
					}
					else
					{
						throw new DataExtractionServiceException(new Problem().code("unknownFile").message("file's not found in '" + csvFileName + "'"));
					}

				}

			}

		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}

		return filesMap;
	}

	/**
	 * @param fileName
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private long getFileSize(String fileName) throws DataExtractionServiceException
	{
		long fileSize = 0;
		XFile xfile = null;
		try
		{
			xfile = getNfsFileByPath(fileName);
			fileSize = xfile.length();
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFileSize").message(e.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFileSize").message(e.getMessage()));
		}
		return fileSize;
	}

	/**
	 * @param ds
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private XFile getNfsFileSystem(DataSource ds) throws DataExtractionServiceException
	{
		XFile xfile = null;
		try
		{
			if( ds == null )
			{
				throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null"));
			}
			final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
			final String host = getConnectionParam(ds, PARAM_HOST_ID);
			final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);

			if( shareName.startsWith("/") && shareName.endsWith("/") )
			{
				if( StringUtils.isNotBlank(host) && StringUtils.isNotBlank(shareName) )
				{
					xfile = getNfsFile(host, shareName, fileName);
				}
			}
			else
			{
				throw new DataExtractionServiceException(new Problem().code("unknownFile").message("share '" + shareName + "' path should start and end with '/'"));
			}

		}
		catch ( MalformedURLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		return xfile;
	}

	/**
	 * @param host
	 * @param shareName
	 * @param fileName
	 * @return
	 * @throws MalformedURLException
	 * @throws DataExtractionServiceException
	 */
	private XFile getNfsFile(String host, String shareName, String fileName) throws MalformedURLException, DataExtractionServiceException
	{
		if( shareName.startsWith("/") && shareName.endsWith("/") )
		{
			String path = "nfs://" + host + shareName + fileName;
			XFile xfile = new XFile(path);
			if( xfile.exists() )
			{
				return xfile;
			}
			else
			{
				return null;
			}
		}
		else
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message("share '" + shareName + "' path should start and end with '/'"));
		}

	}

	/**
	 * @param fileName
	 * @return
	 * @throws MalformedURLException
	 */
	private XFile getNfsFileByPath(String fileName) throws MalformedURLException
	{
		String path = "nfs://" + fileName;
		XFile xfile = new XFile(path);
		if( xfile.exists() )
		{
			return xfile;
		}
		else
		{
			return null;
		}
	}

	/**
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	private XFileInputStream getNfsFileInputStreamReader(String fileName) throws IOException
	{
		return new XFileInputStream(getNfsFileByPath(fileName));
	}

	/**
	 * @param fileName
	 * @return
	 */
	private NamedType getFileNameFromList(String fileName)
	{
		NamedType namedType = new NamedType();
		namedType.setName(fileName);
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		return namedType;
	}

	/**
	 * @param fileName
	 * @param delimiter
	 * @param csvHeaderExists
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<NamedType> getHeadersFromFile(String fileName, String delimiter, boolean csvHeaderExists) throws DataExtractionServiceException
	{
		String type = null;
		CSVRecord csvRecord = null;
		CSVParser csvParser = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{
			csvParser = getCsvParser(fileName, delimiter, csvHeaderExists);

			if( csvParser != null )
			{
				csvRecord = csvParser.iterator().next();

				Set<String> csvHeaderList = new HashSet<>();
				if( csvHeaderExists )
				{
					csvHeaderList = csvParser.getHeaderMap().keySet();
				}
				else
				{
					if( csvRecord != null )
					{
						for (int i = 0; i < csvRecord.size(); i++)
						{
							csvHeaderList.add("Column_" + (i + 1));
						}
					}
				}
				for (String csvHeader : csvHeaderList)
				{
					NamedType attributeForColumn = new NamedType();
					attributeForColumn.setName(csvHeader.trim());

					if( csvHeaderExists )
					{
						type = csvRecord.get(csvHeader).getClass().getSimpleName().toString().toUpperCase();
					}
					else
					{
						type = csvHeader.getClass().getSimpleName().toString().toUpperCase();
					}
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type));
					attributeForColumn.setType(typeForCoumn);
					attributeList.add(attributeForColumn);
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		finally
		{
			try
			{
				if( csvParser != null )
				{
					csvParser.close();
				}
			}
			catch ( IOException e )
			{
				e.printStackTrace();
			}
		}
		return attributeList;
	}

	/**
	 * @param fileName
	 * @param delimiter
	 * @param csvHeaderExists
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private CSVParser getCsvParser(String fileName, String delimiter, boolean csvHeaderExists) throws DataExtractionServiceException
	{

		CSVParser csvParser = null;
		CSVFormat csvFileFormat = null;
		String fileDelimiter = "";
		InputStreamReader fileReader = null;
		try
		{
			fileDelimiter = getDelimiter(fileName, delimiter);
			csvFileFormat = getCsvFormat(fileDelimiter, csvHeaderExists);
			fileReader = new InputStreamReader(getNfsFileInputStreamReader(fileName));
			csvParser = new CSVParser(fileReader, csvFileFormat.withQuote(null));

		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		return csvParser;
	}

	/**
	 * @param fileName
	 * @param delimiter
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private String getDelimiter(String fileName, String delimiter) throws DataExtractionServiceException
	{

		String fileDelimiter = null;
		InputStreamReader inputStreamReader = null;
		BufferedReader bufferedReader = null;
		try
		{
			inputStreamReader = new InputStreamReader(getNfsFileInputStreamReader(fileName));
			bufferedReader = new BufferedReader(inputStreamReader);
			fileDelimiter = bufferedReader.readLine();
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		finally
		{
			try
			{
				if( bufferedReader != null )
				{
					bufferedReader.close();
				}
			}
			catch ( IOException e )
			{
				e.printStackTrace();
			}
		}
		return fileDelimiter;
	}

	/**
	 * @param delimiter
	 * @param csvHeaderExists
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private CSVFormat getCsvFormat(String delimiter, boolean csvHeaderExists) throws DataExtractionServiceException
	{
		CSVFormat csvFileFormat = null;
		if( delimiter.contains(",") )
		{
			csvFileFormat = CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withRecordSeparator("/n").withAllowMissingColumnNames(true);
			_delimiter = ",";
		}
		else if( delimiter.contains(";") )
		{
			csvFileFormat = CSVFormat.DEFAULT.withDelimiter(';').withQuote('"').withRecordSeparator("/n").withAllowMissingColumnNames(true);
			_delimiter = ";";
		}
		else if( delimiter.contains("|") )
		{
			csvFileFormat = CSVFormat.INFORMIX_UNLOAD.withDelimiter('|').withEscape('\\').withQuote('"').withRecordSeparator("\r\n");
			_delimiter = "|";
		}
		else if( delimiter.contains(String.valueOf((char) 9)) || delimiter.equals("\\t") )
		{
			csvFileFormat = CSVFormat.MYSQL.withHeader().withDelimiter((char) 9).withEscape('\\').withIgnoreEmptyLines(false).withQuote(null).withRecordSeparator("\r\n").withNullString("\\N").withQuoteMode(QuoteMode.ALL_NON_NULL);
			_delimiter = String.valueOf((char) 9);
		}
		else if( delimiter.contains("=") )
		{
			csvFileFormat = CSVFormat.POSTGRESQL_TEXT.withDelimiter('\t').withEscape('"').withIgnoreEmptyLines(false).withQuote('"').withRecordSeparator("\r\n").withNullString("\\N").withQuoteMode(QuoteMode.ALL_NON_NULL);
			_delimiter = "=";
		}
		else
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDelimiterType").message("The given '" + delimiter + "' delimiter is not supported in NFS CSV file system"));
		}
		csvFileFormat = csvFileFormat.withAllowMissingColumnNames(true);
		if( csvHeaderExists )
		{
			csvFileFormat = csvFileFormat.withHeader();
		}
		return csvFileFormat;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName,TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new CSVNFSDataExtractionExecutor(context, adapterHome);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}
	public class CSVNFSDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private Map<String, Integer> csvFilesMap = null;

		public CSVNFSDataExtractionExecutor(final DataExtractionContext context, final String adapterHome) throws DataExtractionServiceException
		{
			super(context);
			this.adapterHome = adapterHome;
		}

		@Override
		protected List<DataExtractionTask> runDataExtractionJob() throws Exception
		{
			final DataSource ds = this.context.ds;
			final DataExtractionSpec spec = this.context.spec;
			final DataExtractionJob job = this.context.job;

			final int objectCount;

			synchronized (job)
			{
				objectCount = job.getObjectCount();
			}
			final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
			if( StringUtils.isNotBlank(delimiter) )
			{
				_delimiter = delimiter;
			}
			else
			{
				getDelimiter(ds, spec.getCollection());
			}
			if( !spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				csvFilesMap = getFilesList(ds, spec.getCollection());
			}
			
			return getDataExtractionTasks(ds, spec, job, objectCount, adapterHome, csvFilesMap);
		}
	/**
	 * @param ds
	 * @param spec
	 * @param job
	 * @param rowCount
	 * @param adapterHome
	 * @param csvFilesMap
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, int objectCount, String adapterHome, Map<String, Integer> csvFilesMap)

			throws DataExtractionServiceException
	{

		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();

		try
		{
			if( spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, spec.getCollection(), 1, objectCount));
			}
			else
			{
				for (Map.Entry<String, Integer> entry : csvFilesMap.entrySet())
				{
					dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, entry.getKey(), 1, entry.getValue()));
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}
		return dataExtractionJobTasks;
	}

	/**
	 * @param ds
	 * @param spec
	 * @param job
	 * @param adapterHome
	 * @param filePath
	 * @param offset
	 * @param limit
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, String adapterHome, String filePath, int offset, int limit) throws DataExtractionServiceException
	{
		String columnNames = null;
		String jobName = null;
		String dependencyJar = null;
		String scope = null;
		int sampleSize = 0;
		DataExtractionTask dataExtractionTask = new DataExtractionTask();

		try
		{

			if( spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				scope = String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE);
				sampleSize = spec.getSampleSize();
			}
			else
			{
				scope = String.valueOf(DataExtractionSpec.ScopeEnum.ALL);
				sampleSize = 0;
			}

			final String delimiter = _delimiter;
			final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
			final String fileRegExp = StringUtils.isBlank(getConnectionParam(ds, PARAM_FILE_REG_EXP_ID)) ? "*" : getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			final String host = getConnectionParam(ds, PARAM_HOST_ID);
			final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);

			StringJoiner stringJoiner = new StringJoiner("@#@");
			if( csvHeaderExists )
			{
				columnNames = getFormulateDataSourceColumnNames(ds, spec, ".", "@#@");
			}
			else
			{
				String[] columns = getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").split("@#@");
				
				for (int i = 0; i < columns.length; i++)
				{
					stringJoiner.add(columns[i].substring(columns[i].lastIndexOf("_") + 1));
				}
				
				columnNames = stringJoiner.toString();
			}

			if( csvHeaderExists )
			{
				jobName = JOB_NAME;
				dependencyJar = DEPENDENCY_JAR;
			}
			else
			{
				jobName = JOB_NAME_WithoutHeader;
				dependencyJar = DEPENDENCY_JAR_WithoutHeader;
			}

			String taskId = "DES-Task-" + getUUID();

			Map<String, String> contextParams = getContextParams(adapterHome, jobName,

					columnNames.replaceAll(" ", "\\ "), filePath.replace(host + shareName, "").trim().replaceAll(" ", "\\ "),

					delimiter, job.getOutputMessagingQueue(),

					fileRegExp, host, String.valueOf(0), String.valueOf(0), shareName, String.valueOf(offset), String.valueOf(sampleSize),

					scope, String.valueOf(limit), taskId);

			dataExtractionTask.taskId(taskId)

					.jobId(job.getId())

					.typeId(TYPE_ID + "__" + jobName + "__" + dependencyJar)

					.contextParameters(contextParams)

					.numberOfFailedAttempts(0);
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}
		return dataExtractionTask;
	}

	/**
	 * @param jobFilesPath
	 * @param jobName
	 * @param dataSourceColumnNames
	 * @param dataSourceFilePath
	 * @param dataSourceDelimiter
	 * @param jobId
	 * @param fileRegExp
	 * @param host
	 * @param user
	 * @param password
	 * @param shareName
	 * @param offset
	 * @param limit
	 * @param dataSourceScope
	 * @param dataSourceSampleSize
	 * @param taskId
	 * @return
	 * @throws IOException
	 */
	public Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceColumnNames,

			String dataSourceFilePath, String dataSourceDelimiter, String jobId, String fileRegExp, String host,

			String user, String password, String shareName, String offset, String limit, String dataSourceScope,

			String dataSourceSampleSize, String taskId) throws IOException
	{

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);

		ilParamsVals.put("FILEREGEX", fileRegExp);

		ilParamsVals.put("HOST", host);

		ilParamsVals.put("USERNAME", user);

		ilParamsVals.put("PASSWORD", password);

		ilParamsVals.put("SHARENAME", shareName);

		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);

		ilParamsVals.put("DATASOURCE_FILE_PATH", dataSourceFilePath);

		ilParamsVals.put("DATASOURCE_DELIMITER", dataSourceDelimiter);

		ilParamsVals.put("JOB_ID", jobId);

		ilParamsVals.put("OFFSET", offset);

		ilParamsVals.put("LIMIT", limit);

		ilParamsVals.put("SCOPE", dataSourceScope);

		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		ilParamsVals.put("DESTASKID", taskId);

		ilParamsVals.put("DESTASKSTATUS", taskStatusQueueName);

		return ilParamsVals;

	}
 }
}
