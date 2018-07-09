package com.prifender.des.adapter.hierarchical.hdfsparquet;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oro.text.GlobCompiler;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
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
 * The HDFSParquetDataSourceAdapter Component is a file system , implements
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
public final class HDFSParquetDataSourceAdapter extends DataSourceAdapter
{ 

	private static final String JOB_NAME = "local_project.prifender_hdfs_parquet_m2_v1_0_1.Prifender_HDFS_Parquet_M2_v1";

	private static final String DEPENDENCY_JAR = "prifender_hdfs_parquet_m2_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	public static final String TYPE_ID = "HDFSParquet";
	public static final String TYPE_LABEL = "Apache Parquet";
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
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS Parquet connection successfully established.");
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
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS Avro.");
	}

	private void destroy(Configuration config, FileSystem fileSystem)
	{
		if( config != null )
		{
			try
			{
				fileSystem.close();
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
		catch ( IllegalArgumentException iae )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownFile").message(iae.getMessage()));
		}
		finally
		{
			destroy(config, fileSystem);
		}
		return metadata;
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

	private FileSystem getDataBaseConnection(String hostName, String port, String filePath, String user) throws SQLException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException
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

	private Metadata metadataByConnection(FileSystem fileSystem, String fileName, String fileRegExp) throws DataExtractionServiceException, IOException
	{
		Metadata metadata = new Metadata();
		List<Path> filesList = null;
		try
		{
			if( fileSystem == null )
			{
				throw new IllegalArgumentException("File not found in the given path '" + fileName + "'.");
			}
			Path path = new Path(fileName);
			filesList = getFilesPathList(fileSystem, path, fileRegExp);
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (Path file : filesList)
			{
				ParquetMetadata readFooter = ParquetFileReader.readFooter(config, file, ParquetMetadataConverter.NO_FILTER);
				MessageType schema = readFooter.getFileMetaData().getSchema();
				List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
				for (NamedType namedType : fNameHeaderList)
				{
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getColumnsFromFile(schema);
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
		return metadata;
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

	private List<NamedType> getColumnsFromFile(MessageType schema) throws DataExtractionServiceException
	{
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try
		{

			Set<String> nestedArraycolumns = new HashSet<>();
			Set<String> singleArrayColumns = new HashSet<>();
			Set<String> columns = new HashSet<>();
			List<org.apache.parquet.schema.Type> types = schema.getFields();
			Map<String, String> map = new HashMap<String, String>();
			Map<String, String> columnMap = new HashMap<String, String>();
			for (org.apache.parquet.schema.Type type : types)
			{
				String columnName = type.getName();
				List<ColumnDescriptor> columnDescriptorList = schema.getColumns();
				for (ColumnDescriptor columnDescriptor : columnDescriptorList)
				{
					String columnDesc = columnDescriptor.toString();
					if( columnDesc.contains(columnName) )
					{
						if( type.isPrimitive() == false )
						{
							String result = columnDescriptor.toString().substring(0, columnDescriptor.toString().indexOf("]"));
							String[] names = result.split(",");
							String name = names[names.length - 1];
							if( !name.trim().equals("array_element") )
							{
								nestedArraycolumns.add(columnName);
							}
							else
							{
								singleArrayColumns.add(columnName);
							}
							map.put(columnName + "." + name.trim(), columnDescriptor.getType().toString());
						}
						else
						{
							columns.add(columnName);
							columnMap.put(columnName, columnDescriptor.getType().toString());
						}
					}
				}
			}

			for (String column : columns)
			{
				for (Map.Entry<String, String> entry : columnMap.entrySet())
				{
					if( column.equals(entry.getKey()) )
					{
						NamedType namedType = new NamedType();
						namedType.setName(column);
						Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(entry.getValue().toUpperCase()));
						namedType.setType(typeForCoumn);
						attributeList.add(namedType);
					}
				}
			}

			for (String column : nestedArraycolumns)
			{
				NamedType namedType = new NamedType();
				namedType.setName(column);
				namedType.setType(new Type().kind(Type.KindEnum.LIST));
				List<NamedType> attributeListForColumns = new ArrayList<>();
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				for (Map.Entry<String, String> entry : map.entrySet())
				{
					if( column.equals(entry.getKey().split("\\.")[0]) )
					{
						if( !entry.getKey().split("\\.")[1].equals("array_element") )
						{
							NamedType childNamedType = new NamedType();
							childNamedType.setName(entry.getKey().split("\\.")[1]);
							Type childCloumnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(entry.getValue().toUpperCase()));
							childNamedType.setType(childCloumnType);
							attributeListForColumns.add(childNamedType);
						}
					}
				}
				entryType.setAttributes(attributeListForColumns);
				attributeList.add(namedType);
			}
			for (String column : singleArrayColumns)
			{
				NamedType namedType = new NamedType();
				namedType.setName(column);
				namedType.setType(new Type().kind(Type.KindEnum.LIST));
				for (Map.Entry<String, String> entry : map.entrySet())
				{
					if( column.equals(entry.getKey().split("\\.")[0]) )
					{
						if( entry.getKey().split("\\.")[1].equals("array_element") )
						{
							Type childCloumnType = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(entry.getValue().toUpperCase()));
							namedType.getType().setEntryType(childCloumnType);
						}
					}
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

	@SuppressWarnings("deprecation")
	@Override
	public int getCountRows(DataSource ds,DataExtractionSpec spec) throws DataExtractionServiceException
	{
		int countRows = 0;
		FileSystem fileSystem = null;
		ParquetFileReader parquetFileReader = null;
		try
		{
			String tableName = spec.getCollection();
			fileSystem = getDataBaseConnection(ds);
			Path path = new Path(tableName.trim());
			String pattern = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
			List<Path> filesList = getFilesPathList(fileSystem, path, pattern);

			for (Path filepath : filesList)
			{
				for (Footer f : ParquetFileReader.readFooters(config, filepath))
				{
					for (BlockMetaData b : f.getParquetMetadata().getBlocks())
					{
						countRows += b.getRowCount();
					}
				}
			}
		}
		catch ( ClassNotFoundException | SQLException | InterruptedException | URISyntaxException | IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		finally
		{
			if( parquetFileReader != null )
			{
				try
				{
					parquetFileReader.close();
				}
				catch ( IOException e )
				{
					e.printStackTrace();
				}
			}
			destroy(config, fileSystem);
		}
		return countRows;
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
			final DataExtractionContext context = new DataExtractionContext(this, getDataSourceType(), ds, spec, job, this.messaging, this.pendingTasksQueue, this.pendingTasksQueueName,TYPE_ID, this.encryption);
			final DataExtractionThread dataExtractionExecutor = new HDFSParquetDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class HDFSParquetDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public HDFSParquetDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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

			Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, dataSourceUser,

					hdfsFilePath.replaceAll(" ", "\\ "), getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), 
					
					fileRegExp, "hdfs://" + dataSourceHost, dataSourcePort,

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
	public Map<String, String> getContextParams(String jobFilesPath, String jobName, String hdfsUserName,

			String hdfsFilePath, String dataSourceColumnNames, String fileRegExp,

			String dataSourceHost, String dataSourcePort, String jobId, String offset,

			String limit, String dataSourceScope, String dataSourceSampleSize) throws IOException
	{

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);

		ilParamsVals.put("HDFS_URI", dataSourceHost);

		ilParamsVals.put("HADOOP_USER_NAME", hdfsUserName);

		ilParamsVals.put("HDFS_FILE_PATH", hdfsFilePath);

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