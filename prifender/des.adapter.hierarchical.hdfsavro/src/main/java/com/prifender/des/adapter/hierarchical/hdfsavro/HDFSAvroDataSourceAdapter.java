package com.prifender.des.adapter.hierarchical.hdfsavro;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oro.text.GlobCompiler;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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

/**
 * The HDFSAvroDataSourceAdapter Component is a file system , implements
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
public final class HDFSAvroDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	@Value("${scheduling.taskStatusQueue}")
	private String taskStatusQueueName;

	private static final String JOB_NAME = "local_project.prifender_hdfs_avro_m2_v1_0_1.Prifender_HDFS_Avro_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_hdfs_avro_m2_v1_0_1.jar";

	public static final String TYPE_ID = "HDFSAvro";

	public static final String TYPE_LABEL = "Apache Avro";
	// File

	public static final String PARAM_FILE_ID = "File";
	public static final String PARAM_FILE_LABEL = "File";
	public static final String PARAM_FILE_DESCRIPTION = "The path of the file";

	public static final ConnectionParamDef PARAM_FILE = new ConnectionParamDef().id(PARAM_FILE_ID).label(PARAM_FILE_LABEL).description(PARAM_FILE_DESCRIPTION).type(TypeEnum.STRING);

	// File Regular Expression

	public static final String PARAM_FILE_REG_EXP_ID = "FileRegExp";
	public static final String PARAM_FILE_REG_EXP_LABEL = "File Regular Expression";
	public static final String PARAM_FILE_REG_EXP_DESCRIPTION = "The file regular expression";

	public static final ConnectionParamDef PARAM_FILE_REG_EXP = new ConnectionParamDef().id(PARAM_FILE_REG_EXP_ID).label(PARAM_FILE_REG_EXP_LABEL).description(PARAM_FILE_REG_EXP_DESCRIPTION).type(TypeEnum.STRING).required(false);

	Configuration config;

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(PARAM_PORT).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_FILE).addConnectionParamsItem(PARAM_FILE_REG_EXP);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException
	{
		FileSystem fileSystem = null;
		try
		{
			fileSystem = getDataBaseConnection(ds);
			if( fileSystem != null )
			{
				final Path path = new Path(getConnectionParam(ds, PARAM_FILE_ID));
				final String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
				List<Path> pathList = getFilesPathList(fileSystem, path, pattern);
				if( pathList != null && pathList.size() > 0 )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS Avro connection successfully established.");
				}
				else
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("File's not found in a given file regular expression '" + pattern + "' or file path.");
				}
			}
		}
		catch ( ClassNotFoundException | SQLException | IOException | InterruptedException | URISyntaxException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		finally
		{
			destroy(config, fileSystem, null);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS Avro.");
	}

	public void destroy(Configuration config, FileSystem fileSystem, SeekableInput seekableInput)
	{
		if( config != null )
		{
			try
			{
				if( fileSystem != null )
				{
					fileSystem.close();
				}
				if( seekableInput != null )
				{
					seekableInput.close();
				}
				if( config != null )
				{
					config.clear();
				}

			}
			catch ( IOException e )
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;
		FileSystem fileSystem = null;
		try
		{
			fileSystem = getDataBaseConnection(ds);
			final String filePath = getConnectionParam(ds, PARAM_FILE_ID);
			final String fileRegExp = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			metadata = metadataByConnection(fileSystem, filePath, fileRegExp);
		}
		catch ( ClassNotFoundException | SQLException | IOException | InterruptedException | URISyntaxException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(config, fileSystem, null);
		}
		return metadata;
	}
	
	@Override
	public int getCountRows(DataSource ds,  DataExtractionSpec spec) throws DataExtractionServiceException
	{

		int objectCount = 0;
		Map<String, Integer> csvFilesMap = null;
		try
		{
			String tableName = spec.getCollection();
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

	private FileSystem getDataBaseConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException, IOException, InterruptedException, URISyntaxException
	{
		if( ds == null )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null"));
		}

		final String hostName = getConnectionParam(ds, PARAM_HOST_ID);
		final String portNumber = getConnectionParam(ds, PARAM_PORT_ID);
		final String filePath = getConnectionParam(ds, PARAM_FILE_ID);
		final String user = getConnectionParam(ds, PARAM_USER_ID);

		return getDataBaseConnection(hostName, portNumber, filePath, user);
	}

	private FileSystem getDataBaseConnection(String hostName, String por, String filePath, String user) throws SQLException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException
	{

		FileSystem fileSystem = null;

		File file = new File(".");
		System.getProperties().put("hadoop.home.dir", file.getAbsolutePath());
		new File("./bin").mkdirs();
		new File("./bin/winutils.exe").createNewFile();

		config = new Configuration();
		config.set("fs.defaultFS", "hdfs://" + hostName);
		config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		UserGroupInformation.setConfiguration(config);

		if( StringUtils.isBlank(user) )
		{
			fileSystem = FileSystem.get(config);
		}
		else
		{
			System.setProperty("HADOOP_USER_NAME", user);
			fileSystem = FileSystem.get(new java.net.URI(config.get("fs.defaultFS")), config, user);
		}

		return fileSystem;
	}

	public Metadata metadataByConnection(FileSystem fileSystem, String fileName, String fileRegExp) throws DataExtractionServiceException, IOException
	{
		Metadata metadata = new Metadata();
		List<Path> filesList = null;
		FileReader<GenericRecord> fileReader = null;
		SeekableInput input = null;
		try
		{
			Path path = new Path(fileName);
			filesList = getFilesPathList(fileSystem, path, fileRegExp);
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (Path file : filesList)
			{
				input = new FsInput(file, config);
				DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
				fileReader = DataFileReader.openReader(input, reader);
				/* For Sequence files we are showing folder path */
				List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
				for (NamedType namedType : fNameHeaderList)
				{
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getColumnsFromFile(fileReader);
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
				if( fileSystem.isDirectory(path) )
				{
					return metadata;
				}
				else
				{
					continue;
				}
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadata").message(e.getMessage()));
		}
		finally
		{
			if( fileReader != null )
			{
				fileReader.close();
			}

			if( input != null )
			{
				input.close();
			}
		}
		return metadata;
	}

	@SuppressWarnings("static-access")
	private List<Path> getFilesPathList(FileSystem fileSystem, Path path, String pattern) throws FileNotFoundException, IOException
	{
		List<Path> filePathList = new ArrayList<>();
		FileStatus[] fileStatuses = null;
		if( fileSystem != null )
		{
			if( fileSystem.isDirectory(path) )
			{
				if( StringUtils.isNotBlank(pattern) )
				{
					fileStatuses = listFiles(fileSystem, path, pattern);
				}
				else
				{
					fileStatuses = fileSystem.listStatus(path);
				}
				for (FileStatus fileStatus : fileStatuses)
				{
					if( fileStatus.isFile() )
					{
						filePathList.add(path.getPathWithoutSchemeAndAuthority(fileStatus.getPath()));
					}
				}
			}
			else
			{
				filePathList.add(path);
			}
		}
		return filePathList;
	}

	/** @return FileStatus for data files only. */

	@SuppressWarnings("deprecation")
	private FileStatus[] listFiles(FileSystem fs, Path path, String pattern) throws IOException
	{
		FileStatus[] fileStatuses = fs.listStatus(path);
		List<FileStatus> files = new ArrayList<FileStatus>();
		Pattern patt = Pattern.compile(GlobCompiler.globToPerl5(pattern.toCharArray(), GlobCompiler.DEFAULT_MASK));
		for (FileStatus fstat : fileStatuses)
		{
			String fname = fstat.getPath().getName();
			if( !fstat.isDir() )
			{
				Matcher mat = patt.matcher(fname);
				if( mat.matches() )
				{
					files.add(fstat);
				}
			}
		}
		return (FileStatus[]) files.toArray(new FileStatus[files.size()]);
	}

	private List<NamedType> getFileNameFromList(String fName)
	{
		List<NamedType> tableList = new ArrayList<>();
		NamedType namedType = new NamedType();
		namedType.setName(fName);
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		tableList.add(namedType);
		return tableList;
	}

	private List<NamedType> getColumnsFromFile(FileReader<GenericRecord> fileReader) throws DataExtractionServiceException
	{
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{
			Schema scehma = fileReader.getSchema();
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(scehma.toString());
			JSONArray jsonArray = (JSONArray) json.get("fields");

			for (int i = 0; i < jsonArray.size(); i++)
			{
				JSONObject object = (JSONObject) jsonArray.get(i);
				String columnName = (String) object.get("name");
				NamedType namedType = new NamedType();
				namedType.setName(columnName);
				JSONArray typeArray = (JSONArray) object.get("type");
				if( typeArray.get(1) instanceof JSONObject )
				{
					namedType.setType(new Type().kind(Type.KindEnum.LIST));
					List<NamedType> attributeListForColumns = new ArrayList<>();
					JSONObject subObject = (JSONObject) typeArray.get(1);
					JSONArray subArray = (JSONArray) subObject.get("items");
					if( subArray.get(1) instanceof String )
					{
						String type = (String) subArray.get(1);
						Type childCloumnType = new Type().kind(Type.KindEnum.LIST).dataType(getDataType(type.toUpperCase()));
						namedType.getType().setEntryType(childCloumnType);
					}
					else if( subArray.get(1) instanceof JSONObject )
					{
						Type entryType = new Type().kind(Type.KindEnum.OBJECT);
						namedType.getType().setEntryType(entryType);
						JSONObject subArrayObject = (JSONObject) subArray.get(1);
						JSONArray colmnsAndTypes = (JSONArray) subArrayObject.get("fields");
						for (int j = 0; j < colmnsAndTypes.size(); j++)
						{
							JSONObject objectName = (JSONObject) colmnsAndTypes.get(j);
							String subColumn = (String) objectName.get("name");
							String docType = (String) objectName.get("doc");
							NamedType childNamedType = new NamedType();
							childNamedType.setName(subColumn);
							Type childCloumnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(docType.toUpperCase()));
							childNamedType.setType(childCloumnType);
							attributeListForColumns.add(childNamedType);
						}
						entryType.setAttributes(attributeListForColumns);
					}

				}
				else
				{
					Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(typeArray.get(1).toString().toUpperCase()));
					namedType.setType(typeForCoumn);
				}
				attributeList.add(namedType);
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownMetadata").message(e.getMessage()));
		}

		return attributeList;
	}

	public int getFilesSize(DataSource ds, String tableName) throws DataExtractionServiceException, IOException
	{
		int fileLength = 0;
		FileSystem fileSystem = null;
		SeekableInput seekableInput = null;
		try
		{
			fileSystem = getDataBaseConnection(ds);
			Path path = new Path(tableName.trim());
			String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			List<Path> filesList = getFilesPathList(fileSystem, path, pattern);
			for (Path filepath : filesList)
			{
				seekableInput = new FsInput(filepath, config);
				fileLength = +(int) seekableInput.length();
			}
		}
		catch ( ClassNotFoundException | SQLException | IOException | InterruptedException | URISyntaxException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(config, fileSystem, seekableInput);
		}
		return fileLength;
	}

	public int getFileSize(Path filepath) throws DataExtractionServiceException, IOException
	{
		int fileLength = 0;
		SeekableInput seekableInput = null;
		try
		{
			seekableInput = new FsInput(filepath, config);
			fileLength = +(int) seekableInput.length();
		}
		finally
		{
			destroy(null, null, seekableInput);
		}
		return fileLength;
	}

	private Map<String, Integer> getFilesList(DataSource ds, String tableName) throws DataExtractionServiceException
	{
		FileSystem fileSystem = null;
		Map<String, Integer> filesMap = null;
		try
		{
			fileSystem = getDataBaseConnection(ds);
			Path path = new Path(tableName.trim());
			String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			filesMap = getFilesList(fileSystem, path, pattern);
		}
		catch ( ClassNotFoundException | SQLException | IOException | InterruptedException | URISyntaxException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(config, fileSystem, null);
		}
		return filesMap;

	}

	@SuppressWarnings("static-access")
	private Map<String, Integer> getFilesList(FileSystem fileSystem, Path path, String pattern) throws FileNotFoundException, IOException, DataExtractionServiceException
	{
		List<Path> filePathList = new ArrayList<>();
		FileStatus[] fileStatuses = null;
		Map<String, Integer> filesMap = new HashMap<String, Integer>();
		if( fileSystem != null )
		{
			if( fileSystem.isDirectory(path) )
			{
				if( StringUtils.isNotBlank(pattern) )
				{
					fileStatuses = listFiles(fileSystem, path, pattern);
				}
				else
				{
					fileStatuses = fileSystem.listStatus(path);
				}
				for (FileStatus fileStatus : fileStatuses)
				{
					if( fileStatus.isFile() )
					{
						filePathList.add(path.getPathWithoutSchemeAndAuthority(fileStatus.getPath()));
					}
				}
			}
			else
			{
				filePathList.add(path);
			}

			if( filePathList != null && filePathList.size() > 0 )
			{
				for (Path fileName : filePathList)
				{
					filesMap.put(fileName.toString(), (int) getFileSize(fileName));
				}
			}

		}
		return filesMap;
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
			final DataExtractionThread dataExtractionExecutor = new HDFSAvroDataExtractionExecutor(context, adapterHome);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class HDFSAvroDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private Map<String, Integer> filesMap = null;

		public HDFSAvroDataExtractionExecutor(final DataExtractionContext context, final String adapterHome) throws DataExtractionServiceException
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
			if( !spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				filesMap = getFilesList(ds, spec.getCollection());
			}
			
			return getDataExtractionTasks(ds, spec, job, objectCount, adapterHome, filesMap);
		}

	/**
	 * @param ds
	 * @param spec
	 * @param job
	 * @param rowCount
	 * @param adapterHome
	 * @param containersCount
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, int rowCount, String adapterHome, Map<String, Integer> filesMap)

			throws DataExtractionServiceException
	{

		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();

		try
		{
			if( spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE) )
			{
				dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, spec.getCollection(), 1, rowCount));
			}
			else
			{
				for (Map.Entry<String, Integer> entry : filesMap.entrySet())
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
	 * @param offset
	 * @param limit
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, String adapterHome, String filePath, int offset, int limit)

			throws DataExtractionServiceException
	{
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

			final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID);

			final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);

			final String dataSourceUser = getConnectionParam(ds, PARAM_USER_ID);

			final String fileRegExp = StringUtils.isBlank(getConnectionParam(ds, PARAM_FILE_REG_EXP_ID)) ? "*" : getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);

			String taskId = "DES-Task-" + getUUID();

			Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser, filePath.replaceAll(" ", "\\ "),

					getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), fileRegExp, "hdfs://" + dataSourceHost, dataSourcePort,

					job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(sampleSize),

					scope, String.valueOf(limit), taskId);

			dataExtractionTask.taskId(taskId)

					.jobId(job.getId())

					.typeId(TYPE_ID + "__" + JOB_NAME + "__" + DEPENDENCY_JAR)

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
	 * @param hdfsUserName
	 * @param hdfsFilePath
	 * @param dataSourceColumnNames
	 * @param fileRegExp
	 * @param dataSourceHost
	 * @param dataSourcePort
	 * @param jobId
	 * @param offset
	 * @param limit
	 * @param dataSourceScope
	 * @param dataSourceSampleSize
	 * @return
	 * @throws IOException
	 */
	private Map<String, String> getContextParams(String jobFilesPath, String jobName, String hdfsUserName,

			String hdfsFilePath, String dataSourceColumnNames, String fileRegExp,

			String dataSourceHost, String dataSourcePort, String jobId, String offset, String limit,

			String dataSourceScope, String dataSourceSampleSize, String taskId) throws IOException
	{

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);

		ilParamsVals.put("Cont_File", "");

		ilParamsVals.put("HDFS_URI", dataSourceHost);

		ilParamsVals.put("HADOOP_USER_NAME", hdfsUserName);

		ilParamsVals.put("HDFS_FILE_PATH", hdfsFilePath);

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);

		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);

		ilParamsVals.put("FILEREGEX", fileRegExp);

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