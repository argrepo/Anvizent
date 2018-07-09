package com.prifender.des.adapter.hierarchical.hdfsorc;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.security.UserGroupInformation;
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

/**
 * The HDFSORCDataSourceAdapter is a File System Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch hierarchical metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-05-09
 */

@Component
public final class HDFSORCDataSourceAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final String TYPE_ID = "HDFSORC";

	public static final String TYPE_LABEL = "Apache ORC";

	private static final String JOB_NAME = "local_project.prifender_hdfs_orc_m3_v1_0_1.Prifender_HDFS_ORC_M3_v1";

	private static final String DEPENDENCY_JAR = "prifender_hdfs_orc_m3_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

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

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_HOST).addConnectionParamsItem(PARAM_PORT).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_FILE).addConnectionParamsItem(PARAM_FILE_REG_EXP);

	Configuration config;

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
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS ORC connection successfully established.");
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
			destroy(config, fileSystem);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS ORC.");
	}

	/**
	 * Close the file System if available and clear's the configuration
	 * 
	 * @param config
	 * @param fileSystem
	 */
	private void destroy(Configuration config, FileSystem fileSystem)
	{
		if( config != null )
		{
			try
			{
				if( fileSystem != null )
				{
					fileSystem.close();
				}
				config.clear();
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
			destroy(config, fileSystem);
		}
		return metadata;
	}

	/**
	 * Gets the Required connection Parameters inorder to get the Path
	 * 
	 * @param ds
	 * @return Path
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws DataExtractionServiceException
	 * @throws IOException
	 */
	public FileSystem getDataBaseConnection(DataSource ds) throws SQLException, ClassNotFoundException, DataExtractionServiceException, IOException, InterruptedException, URISyntaxException
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

	/**
	 * Returns the Path,based on the given connection parameters
	 * 
	 * @param hostName
	 * @param port
	 * @param dfsReplication
	 * @param filePath
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */

	public FileSystem getDataBaseConnection(String hostName, String port, String filePath, String user) throws SQLException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException
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

	/**
	 * Based on the StructObject Inspector it returns the metadata for the ORC
	 * File
	 * 
	 * @param path
	 * @param fileName
	 * @return Metadata
	 * @throws DataExtractionServiceException
	 * @throws IOException
	 */
	public Metadata metadataByConnection(FileSystem fileSystem, String fileName, String fileRegExp) throws DataExtractionServiceException, IOException
	{
		Metadata metadata = new Metadata();
		List<String> fileNameList = new ArrayList<>();
		Reader fileReader = null;
		List<Path> filesList = null;
		try
		{

			Path path = new Path(fileName);
			filesList = getFilesPathList(fileSystem, path, fileRegExp);

			for (Path file : filesList)
			{

				fileNameList.add(fileName);
				ReaderOptions opts = OrcFile.readerOptions(config);
				fileReader = OrcFile.createReader(file, opts);
				if( fileReader != null )
				{

					StructObjectInspector inspector = (StructObjectInspector) fileReader.getObjectInspector();
					List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();

					NamedType namedType = getFileNameFromList(fileName);
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);

					List<NamedType> attributeListForColumns = getColumnsForFile(fileReader, inspector);
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					metadata.setObjects(namedTypeObjectsList);
				}
			}
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	/**
	 * Returns the number of rows available for a given Reader
	 * 
	 * @param ds
	 * @param tableName
	 * @return
	 * @throws DataExtractionServiceException
	 */
	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		FileSystem fileSystem = null;
		Reader fileReader = null;
		try
		{
			String tableName = spec.getCollection();
			fileSystem = getDataBaseConnection(ds);
			Path path = new Path(tableName.trim());
			String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			List<Path> filesList = getFilesPathList(fileSystem, path, pattern);

			for (Path filepath : filesList)
			{
				ReaderOptions opts = OrcFile.readerOptions(config);
				fileReader = OrcFile.createReader(filepath, opts);
				if( fileReader != null )
				{
					countRows = countRows + getRowCount(fileReader, tableName);
				}
			}
		}
		catch ( ClassNotFoundException | SQLException | IOException | InterruptedException | URISyntaxException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			destroy(config, fileSystem);
		}
		return countRows;
	}

	private List<NamedType> getColumnsForFile(Reader fileReader, StructObjectInspector inspector)
	{
		List<NamedType> namedTypeList = new ArrayList<NamedType>();

		StructObjectInspector structObjectInspector = (StructObjectInspector) fileReader.getObjectInspector();
		if( structObjectInspector != null )
		{

			List<? extends StructField> structFieldList = structObjectInspector.getAllStructFieldRefs();
			if( structFieldList != null && structFieldList.size() > 0 )
			{
				for (StructField structField : structFieldList)
				{

					NamedType namedType = new NamedType();
					String structFieldColName = structField.getFieldName();
					ObjectInspector objectInspector = structField.getFieldObjectInspector();

					String type = objectInspector.getTypeName().toUpperCase();
					Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type));

					String category = objectInspector.getCategory().toString();

					if( StringUtils.equals(category, ObjectInspector.Category.LIST.toString()) )
					{

						namedType.setName(structFieldColName);
						namedType.setType(new Type().kind(Type.KindEnum.LIST));
						Type entryType = new Type().kind(Type.KindEnum.OBJECT);
						namedType.getType().setEntryType(entryType);

						List<NamedType> attributeListForColumns = getSubObjectFieldList(objectInspector);
						entryType.setAttributes(attributeListForColumns);
						namedTypeList.add(namedType);

					}
					else
					{
						namedType.setName(structFieldColName);
						namedType.setType(columnType);
						namedTypeList.add(namedType);
					}
				}
			}
		}
		return namedTypeList;
	}

	/**
	 * From the given objectInspector of the orcFile it gets the List of
	 * ObjectInspector and from that List based on the DataType it gets the
	 * StructobjectInspector and Returns the Column data
	 * 
	 * @param objectInspector
	 * @return List<namedType>
	 */
	private List<NamedType> getSubObjectFieldList(ObjectInspector objectInspector)
	{
		List<NamedType> namedTypeList = new ArrayList<NamedType>();

		ListObjectInspector subListObjectInspector = (ListObjectInspector) objectInspector;
		String category = subListObjectInspector.getListElementObjectInspector().getTypeName();
		if( StringUtils.equals(category.toUpperCase(), "STRING") )
		{
			NamedType namedType = new NamedType();
			Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(category.toUpperCase()));
			namedType.setType(columnType);
			namedTypeList.add(namedType);
		}
		else
		{
			StructObjectInspector subStructObjectInspector = (StructObjectInspector) subListObjectInspector.getListElementObjectInspector();
			if( subStructObjectInspector != null )
			{
				List<? extends StructField> subStructFieldList = subStructObjectInspector.getAllStructFieldRefs();
				if( subStructFieldList != null && subStructFieldList.size() > 0 )
				{
					for (StructField subStructField : subStructFieldList)
					{
						NamedType namedType = new NamedType();
						ObjectInspector subObjectInspector = subStructField.getFieldObjectInspector();
						String type = subObjectInspector.getTypeName().toUpperCase();
						Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type));
						namedType.setName(subStructField.getFieldName());
						namedType.setType(columnType);
						namedTypeList.add(namedType);
					}
				}
			}
		}
		return namedTypeList;
	}

	/**
	 * Returns the NamedType which contains the filename and the Type of the
	 * File
	 * 
	 * @param fileName
	 * @return NamedType
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
	 * Returns the NumberOfRows for a given Reader in Orc
	 * 
	 * @param reader
	 * @param fName
	 * @return
	 */
	private int getRowCount(Reader reader, String fName)
	{
		return (int) reader.getNumberOfRows();
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

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final DataExtractionJob job = createDataExtractionJob(ds, spec);
			String adapterHome = createDir(this.desHome, TYPE_ID);
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName, TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new HDFSORCDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class HDFSORCDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public HDFSORCDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
		{
			super(context);
			this.adapterHome = adapterHome;
			this.containersCount = containersCount;
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
			return getDataExtractionTasks(ds, spec, job, objectCount, adapterHome, containersCount);
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

				DataExtractionJob job, int objectCount, String adapterHome, int containersCount) throws DataExtractionServiceException
		{

			List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();

			try
			{
				if( objectCount <= MIN_THRESHOULD_ROWS )
				{
					dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, 1, objectCount));
				}
				else
				{
					int taskSampleSize = generateTaskSampleSize(objectCount, containersCount);

					if( taskSampleSize <= MIN_THRESHOULD_ROWS )
					{
						taskSampleSize = MIN_THRESHOULD_ROWS;
					}
					if( taskSampleSize > MAX_THRESHOULD_ROWS )
					{
						taskSampleSize = MAX_THRESHOULD_ROWS;
					}

					int noOfTasks = objectCount / taskSampleSize;

					int remainingSampleSize = objectCount % taskSampleSize;

					for (int i = 0; i < noOfTasks; i++)
					{

						int offset = taskSampleSize * i + 1;

						dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, offset, taskSampleSize));

					}

					if( remainingSampleSize > 0 )
					{
						int offset = noOfTasks * taskSampleSize + 1;

						dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, offset, remainingSampleSize));

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

				DataExtractionJob job, String adapterHome, int offset, int limit) throws DataExtractionServiceException
		{

			DataExtractionTask dataExtractionTask = new DataExtractionTask();

			try
			{

				final String dataSourceHost = getConnectionParam(ds, PARAM_HOST_ID);

				final String dataSourcePort = getConnectionParam(ds, PARAM_PORT_ID);

				final String dataSourceUser = getConnectionParam(ds, PARAM_USER_ID);

				final String hdfsFilePath = getConnectionParam(ds, PARAM_FILE_ID);

				final String fileRegExp = StringUtils.isBlank(getConnectionParam(ds, PARAM_FILE_REG_EXP_ID)) ? "*" : getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);

				Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser, hdfsFilePath,

						getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), fileRegExp, "hdfs://" + dataSourceHost, dataSourcePort,

						job.getOutputMessagingQueue(), String.valueOf(offset), String.valueOf(limit),

						String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE), String.valueOf(limit));

				dataExtractionTask.taskId("DES-Task-" + getUUID())

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

				String dataSourceScope, String dataSourceSampleSize) throws IOException
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

			return ilParamsVals;

		}
	}
}