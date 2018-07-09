package com.prifender.des.adapter.filesystem.csv;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

/**
 * The CSVDataSourceAdapter Component is a file system , implements
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
public final class CSVDataSourceAdapter extends DataSourceAdapter
{

	@Value("${scheduling.taskStatusQueue}")
	private String taskStatusQueueName;

	private static final String JOB_NAME = "local_project.prifender_csv_v2_0_1.Prifender_CSV_v2";

	private static final String DEPENDENCY_JAR = "prifender_csv_v2_0_1.jar";

	private static final String JOB_NAME_WithoutHeader = "local_project.prifender_csv_without_header_v2_0_1.Prifender_CSV_Without_Header_v2";

	private static final String DEPENDENCY_JAR_WithoutHeader = "prifender_csv_without_header_v2_0_1.jar";

	private static final String SMB_JOB_NAME = "local_project.prifender_csv_smb_v2_0_1.Prifender_CSV_SMB_v2";

	private static final String SMB_DEPENDENCY_JAR = "prifender_csv_smb_v2_0_1.jar";

	private static final String SMB_JOB_NAME_WithoutHeader = "local_project.prifender_csv_smb_without_header_v2_0_1.Prifender_CSV_SMB_Without_Header_v2";

	private static final String SMB_DEPENDENCY_JAR_WithoutHeader = "prifender_csv_smb_without_header_v2_0_1.jar";

	public static final String TYPE_ID = "CSV";
	public static final String TYPE_LABEL = "CSV";

	// File

	public static final String PARAM_FILE_ID = "File";
	public static final String PARAM_FILE_LABEL = "File";
	public static final String PARAM_FILE_DESCRIPTION = "The path of the file";

	public static final ConnectionParamDef PARAM_FILE = new ConnectionParamDef().id(PARAM_FILE_ID).label(PARAM_FILE_LABEL).description(PARAM_FILE_DESCRIPTION).type(TypeEnum.STRING);

	// ShareName

	public static final String PARAM_SHARE_ID = "Share";
	public static final String PARAM_SHARE_LABEL = "Share";
	public static final String PARAM_SHARE_DESCRIPTION = "The Name File Share";

	public static final ConnectionParamDef PARAM_SHARE_NAME = new ConnectionParamDef().id(PARAM_SHARE_ID).label(PARAM_SHARE_LABEL).description(PARAM_SHARE_DESCRIPTION).type(TypeEnum.STRING).required(false);

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

	// smb authentication

	public static final String PARAM_SMB_AUTHENTICATION_ID = "SmbAuthentication";
	public static final String PARAM_SMB_AUTHENTICATION_LABEL = "Smb Authentication";
	public static final String PARAM_SMB_AUTHENTICATION_DESCRIPTION = "The Smb Authentication for file";

	public static final ConnectionParamDef PARAM_SMB_AUTHENTICATION = new ConnectionParamDef().id(PARAM_SMB_AUTHENTICATION_ID).label(PARAM_SMB_AUTHENTICATION_LABEL).description(PARAM_SMB_AUTHENTICATION_DESCRIPTION).type(TypeEnum.BOOLEAN);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_FILE).addConnectionParamsItem(clone(PARAM_HOST).required(false)).addConnectionParamsItem(PARAM_SHARE_NAME).addConnectionParamsItem(clone(PARAM_USER).required(false))
			.addConnectionParamsItem(clone(PARAM_PASSWORD).required(false)).addConnectionParamsItem(PARAM_SMB_AUTHENTICATION).addConnectionParamsItem(PARAM_DELIMITER).addConnectionParamsItem(PARAM_FILE_REG_EXP).addConnectionParamsItem(PARAM_HEADER_EXISTS);

	String _delimiter = "";

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		SmbFile smbFile = null;
		File file = null;
		List<String> listFiles = null;
		try
		{
			final String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			final boolean smbAuth = Boolean.parseBoolean(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));
			if( smbAuth )
			{
				smbFile = getSmbFileSystem(ds);
				if( smbFile != null )
				{
					listFiles = getSmbFilesList(smbFile, pattern);
					if( listFiles != null && listFiles.size() > 0 )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Smb CSV File System connection successfully established.");
					}
					else
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("File's not found in a given file regular expression '" + pattern + "' or file path.");
					}
				}
			}
			else
			{
				file = getFileSystem(ds);
				if( file != null )
				{
					listFiles = getFilesList(file, pattern);
					if( listFiles != null && listFiles.size() > 0 )
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("CSV File System connection successfully established.");
					}
					else
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("File's not found in a given file regular expression '" + pattern + "' or file path.");
					}
				}
			}
		}
		catch ( Exception e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Smb / CSV File System.");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{

		final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
		final String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final String user = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String ShareName = getConnectionParam(ds, PARAM_SHARE_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
		final boolean smbAuth = Boolean.valueOf(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));

		return getMetadata(fileName, pattern, delimiter, csvHeaderExists, user, password, host, ShareName, smbAuth);

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
	 * @param smbFile
	 * @param pattern
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private List<String> getSmbFilesList(SmbFile smbFile, String pattern) throws FileNotFoundException, IOException
	{
		SmbFile[] listFiles = null;
		List<String> filesList = new ArrayList<>();
		if( smbFile != null )
		{
			if( smbFile.isDirectory() )
			{
				if( StringUtils.isNotBlank(pattern) )
				{
					listFiles = listSmbFiles(smbFile, pattern);
				}
				else
				{
					listFiles = smbFile.listFiles();
				}
				for (SmbFile fileStatus : listFiles)
				{
					if( fileStatus.isFile() )
					{
						filesList.add(fileStatus.getPath());
					}
				}
			}
			else
			{
				filesList.add(smbFile.getPath());
			}
		}
		return filesList;
	}

	/**
	 * @param file
	 * @param pattern
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private List<String> getFilesList(File file, String pattern) throws FileNotFoundException, IOException
	{
		File[] listFiles = null;
		List<String> filesList = new ArrayList<>();
		if( file != null )
		{
			if( file.isDirectory() )
			{
				if( StringUtils.isNotBlank(pattern) )
				{
					listFiles = listFiles(file, pattern);
				}
				else
				{
					listFiles = file.listFiles();
				}
				for (File fileStatus : listFiles)
				{
					if( fileStatus.isFile() )
					{
						filesList.add(fileStatus.getPath());
					}
				}
			}
			else
			{
				filesList.add(file.getPath());
			}
		}
		return filesList;
	}

	/**
	 * @param smbFile
	 * @param pattern
	 * @return
	 * @throws IOException
	 */
	private File[] listFiles(File smbFile, String pattern) throws IOException
	{
		File[] fileList = smbFile.listFiles();
		List<File> files = new ArrayList<File>();
		Pattern patt = Pattern.compile(GlobCompiler.globToPerl5(pattern.toCharArray(), GlobCompiler.DEFAULT_MASK));
		for (File file : fileList)
		{
			String fname = file.getName();
			if( !file.isDirectory() )
			{
				Matcher mat = patt.matcher(fname);
				if( mat.matches() )
				{
					files.add(file);
				}
			}
		}
		return (File[]) files.toArray(new File[files.size()]);
	}

	/**
	 * @param smbFile
	 * @param pattern
	 * @return
	 * @throws IOException
	 */
	private SmbFile[] listSmbFiles(SmbFile smbFile, String pattern) throws IOException
	{
		SmbFile[] listFiles = smbFile.listFiles();
		List<SmbFile> files = new ArrayList<SmbFile>();
		Pattern patt = Pattern.compile(GlobCompiler.globToPerl5(pattern.toCharArray(), GlobCompiler.DEFAULT_MASK));
		for (SmbFile file : listFiles)
		{
			String fname = file.getName();
			if( !file.isDirectory() )
			{
				Matcher mat = patt.matcher(fname);
				if( mat.matches() )
				{
					files.add(file);
				}
			}
		}
		return (SmbFile[]) files.toArray(new SmbFile[files.size()]);
	}

	/**
	 * @param fileName
	 * @param pattern
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param userName
	 * @param password
	 * @param host
	 * @param smbShareName
	 * @param smbAuth
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private Metadata getMetadata(String fileName, String pattern, String delimiter, boolean csvHeaderExists, String userName, String password, String host, String smbShareName, boolean smbAuth) throws DataExtractionServiceException
	{
		List<String> filesList = null;
		SmbFile smbFile = null;
		File file = null;
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		try
		{
			if( smbAuth && StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) && StringUtils.isNotBlank(smbShareName) )
			{
				smbFile = getSmbFile(userName, password, host, smbShareName, fileName);
				filesList = getSmbFilesList(smbFile, pattern);
			}
			else
			{
				file = getFile(fileName);
				filesList = getFilesList(file, pattern);
			}
			if( filesList.size() > 0 && filesList != null )
			{
				for (String fName : filesList)
				{

					NamedType namedType = getFileNameFromList(fileName);
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getHeadersFromFile(fName, delimiter, csvHeaderExists, smbAuth, userName + ":" + password);

					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					metadata.setObjects(namedTypeObjectsList);

					if( smbAuth )
					{
						if( smbFile.isDirectory() )
						{
							break;
						}
						else
						{
							continue;
						}
					}
					else
					{
						if( file.isDirectory() )
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
		}
		catch ( SmbException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
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
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
		final String user = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
		final boolean smbAuth = Boolean.valueOf(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));
		return getCsvFilesSize(tableName, regex, delimiter, csvHeaderExists, host, shareName, user, password, smbAuth);
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
		final String user = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
		final boolean smbAuth = Boolean.valueOf(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));
		getCsvDelimiter(tableName, regex, delimiter, csvHeaderExists, host, shareName, user, password, smbAuth);
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
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
		final String user = getConnectionParam(ds, PARAM_USER_ID);
		final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
		final String host = getConnectionParam(ds, PARAM_HOST_ID);
		final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
		final boolean smbAuth = Boolean.valueOf(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));
		return getCsvFilesList(tableName, regex, delimiter, csvHeaderExists, host, shareName, user, password, smbAuth);
	}

	/**
	 * @param csvFileName
	 * @param regex
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param host
	 * @param shareName
	 * @param user
	 * @param password
	 * @param smbAuth
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Map<String, Integer> getCsvFilesList(String csvFileName, String regex, String delimiter, boolean csvHeaderExists, String host, String shareName, String user, String password, boolean smbAuth) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		SmbFile smbFile = null;
		List<String> csvFilesList = null;
		File file = null;
		Map<String, Integer> filesMap = new HashMap<String, Integer>();
		try
		{
			if( smbAuth && StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) )
			{
				smbFile = getSmbFile(user, password, host, shareName, csvFileName);
				csvFilesList = getSmbFilesList(smbFile, regex);
			}
			else
			{
				file = getFile(csvFileName);
				csvFilesList = getFilesList(file, regex);
			}
			for (String fileName : csvFilesList)
			{
				filesMap.put(fileName, (int) getFileSize(fileName, delimiter, smbAuth, user + ":" + password));
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}

		return filesMap;
	}

	/**
	 * @param csvFileName
	 * @param regex
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param host
	 * @param shareName
	 * @param user
	 * @param password
	 * @param smbAuth
	 * @return
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private long getCsvFilesSize(String csvFileName, String regex, String delimiter, boolean csvHeaderExists, String host, String shareName, String user, String password, boolean smbAuth) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		int filesSize = 0;
		SmbFile smbFile = null;
		List<String> csvFilesList = null;
		File file = null;
		try
		{
			if( smbAuth && StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) )
			{
				smbFile = getSmbFile(user, password, host, shareName, csvFileName);
				csvFilesList = getSmbFilesList(smbFile, regex);
			}
			else
			{
				file = getFile(csvFileName);
				csvFilesList = getFilesList(file, regex);
			}
			for (String fileName : csvFilesList)
			{
				filesSize += getFileSize(fileName, delimiter, smbAuth, user + ":" + password);
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFileSize").message(e.getMessage()));
		}

		return filesSize;
	}

	/**
	 * @param fileName
	 * @param delimiter
	 * @param smbAuth
	 * @param userNamePassword
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private long getFileSize(String fileName, String delimiter, boolean smbAuth, String userNamePassword) throws DataExtractionServiceException
	{
		long fileSize = 0;
		SmbFile smbFile = null;
		File file = null;
		try
		{
			if( smbAuth )
			{
				smbFile = getSmbFileByPath(userNamePassword, fileName);
				fileSize = smbFile.length();
			}
			else
			{
				file = getFile(fileName);
				fileSize = file.length();
			}

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
	 * @param csvFileName
	 * @param regex
	 * @param delimiter
	 * @param csvHeaderExists
	 * @param host
	 * @param shareName
	 * @param user
	 * @param password
	 * @param smbAuth
	 * @throws DataExtractionServiceException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void getCsvDelimiter(String csvFileName, String regex, String delimiter, boolean csvHeaderExists, String host, String shareName, String user, String password, boolean smbAuth) throws DataExtractionServiceException, FileNotFoundException, IOException
	{
		SmbFile smbFile = null;
		List<String> csvFilesList = null;
		File file = null;
		try
		{
			if( smbAuth && StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) )
			{
				smbFile = getSmbFile(user, password, host, shareName, csvFileName);
				csvFilesList = getSmbFilesList(smbFile, regex);
			}
			else
			{
				file = getFile(csvFileName);
				csvFilesList = getFilesList(file, regex);
			}
			for (String fileName : csvFilesList)
			{
				getCsvFormat(getDelimiter(fileName, delimiter, smbAuth, user + ":" + password), csvHeaderExists);
				break;
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
	}

	/**
	 * @param ds
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private SmbFile getSmbFileSystem(DataSource ds) throws DataExtractionServiceException
	{
		SmbFile smbFile = null;
		try
		{
			if( ds == null )
			{
				throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
			}
			final String fileName = getConnectionParam(ds, PARAM_FILE_ID);

			final String userName = getConnectionParam(ds, PARAM_USER_ID);
			final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
			final String host = getConnectionParam(ds, PARAM_HOST_ID);
			final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
			if( StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) && StringUtils.isNotBlank(shareName) )
			{
				smbFile = getSmbFile(userName, password, host, shareName, fileName);
			}
		}
		catch ( MalformedURLException e )
		{
			throw new DataExtractionServiceException(new Problem().code("Error").message(e.getMessage()));
		}
		return smbFile;
	}

	/**
	 * @param ds
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private File getFileSystem(DataSource ds) throws DataExtractionServiceException
	{
		File file = null;
		try
		{
			final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
			file = getFile(fileName);
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(e.getMessage()));
		}
		return file;
	}

	/**
	 * @param userName
	 * @param password
	 * @param host
	 * @param smbShareName
	 * @param fileName
	 * @return
	 * @throws MalformedURLException
	 */
	private SmbFile getSmbFile(String userName, String password, String host, String smbShareName, String fileName) throws MalformedURLException
	{
		NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(userName + ":" + password);
		String path = "smb://" + host + "/" + smbShareName + "/" + fileName;
		return new SmbFile(path, auth);
	}

	/**
	 * @param userNamePassword
	 * @param path
	 * @return
	 * @throws MalformedURLException
	 */
	private SmbFile getSmbFileByPath(String userNamePassword, String path) throws MalformedURLException
	{
		NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(userNamePassword);
		return new SmbFile(path, auth);
	}

	/**
	 * @param fileName
	 * @return
	 */
	private File getFile(String fileName)
	{
		return new File(fileName);

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
	 * @param smbAuth
	 * @param userNamePassword
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<NamedType> getHeadersFromFile(String fileName, String delimiter, boolean csvHeaderExists, boolean smbAuth, String userNamePassword) throws DataExtractionServiceException
	{
		String type = null;
		CSVRecord csvRecord = null;
		CSVParser csvParser = null;
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{
			csvParser = getCsvParser(fileName, delimiter, csvHeaderExists, smbAuth, userNamePassword);

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
	 * @param smbAuth
	 * @param userNamePassword
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private CSVParser getCsvParser(String fileName, String delimiter, boolean csvHeaderExists, boolean smbAuth, String userNamePassword) throws DataExtractionServiceException
	{

		CSVParser csvParser = null;
		CSVFormat csvFileFormat = null;
		String fileDelimiter = "";
		InputStreamReader fileReader = null;
		try
		{
			fileDelimiter = getDelimiter(fileName, delimiter, smbAuth, userNamePassword);
			csvFileFormat = getCsvFormat(fileDelimiter, csvHeaderExists);
			if( smbAuth )
			{
				fileReader = new InputStreamReader(getSmbFileByPath(userNamePassword, fileName).getInputStream());
			}
			else
			{
				fileReader = new FileReader(fileName);
			}
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

	private String getDelimiter(String fileName, String delimiter, boolean smbAuth, String userNamePassword) throws DataExtractionServiceException
	{

		String fileDelimiter = "";
		InputStreamReader inputStreamReader = null;
		BufferedReader bufferedReader = null;
		try
		{

			if( smbAuth )
			{
				inputStreamReader = new InputStreamReader(getSmbFileByPath(userNamePassword, fileName).getInputStream());
				bufferedReader = new BufferedReader(inputStreamReader);
			}
			else
			{
				bufferedReader = new BufferedReader(new FileReader(fileName));
			}
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
			throw new DataExtractionServiceException(new Problem().code("unknownDelimiter").message("The given '" + delimiter + "' delimiter is not supported in NFS CSV file system"));
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
			final DataExtractionThread dataExtractionExecutor = new CSVDataExtractionExecutor(context, adapterHome);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class CSVDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private Map<String, Integer> csvFilesMap = null;

		public CSVDataExtractionExecutor(final DataExtractionContext context, final String adapterHome) throws DataExtractionServiceException
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
			String fileName = spec.getCollection();
			final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
			if( StringUtils.isNotBlank(delimiter) )
			{
				_delimiter = delimiter;
			}
			else
			{
				getDelimiter(ds, fileName);
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

				DataExtractionJob job, String adapterHome, String filePath, int offset, int limit)

				throws DataExtractionServiceException
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
				final String user = getConnectionParam(ds, PARAM_USER_ID);
				final String password = getConnectionParam(ds, PARAM_PASSWORD_ID);
				final String shareName = getConnectionParam(ds, PARAM_SHARE_ID);
				final boolean smbAuth = Boolean.parseBoolean(getConnectionParam(ds, PARAM_SMB_AUTHENTICATION_ID));

				boolean isSmbFileExists = false;
				if( smbAuth && StringUtils.isNotBlank(shareName) && StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && StringUtils.isNotBlank(host) )
				{
					isSmbFileExists = true;
				}
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
					if( isSmbFileExists )
					{
						jobName = SMB_JOB_NAME;
						dependencyJar = SMB_DEPENDENCY_JAR;
					}
					else
					{
						jobName = JOB_NAME;
						dependencyJar = DEPENDENCY_JAR;
					}

				}
				else
				{
					if( isSmbFileExists )
					{
						jobName = SMB_JOB_NAME_WithoutHeader;
						dependencyJar = SMB_DEPENDENCY_JAR_WithoutHeader;
					}
					else
					{
						jobName = JOB_NAME_WithoutHeader;
						dependencyJar = DEPENDENCY_JAR_WithoutHeader;
					}
				}

				String taskId = "DES-Task-" + getUUID();

				Map<String, String> contextParams = getContextParams(adapterHome, jobName,

						columnNames.replaceAll(" ", "\\ "),

						filePath.replace("smb://" + host + "/" + shareName + "/", "").trim().replaceAll(" ", "\\ "), delimiter,

						job.getOutputMessagingQueue(), fileRegExp, host, user, password,

						shareName != null ? shareName.replaceAll(" ", "\\ ") : null, String.valueOf(offset), String.valueOf(sampleSize),

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
