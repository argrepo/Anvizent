package com.prifender.des.adapter.simplified.dynamodb;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getFormulateDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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
import com.prifender.des.model.Type.DataTypeEnum;

/**
 * The DynamoDBDataSourceAdapter Component implements
 * 
 * Test connection establishment based on datasource
 * 
 * Fetch simplified metadata based on datasource
 * 
 * Start data extraction job based on datasource and data extraction spec
 * 
 * @author Mahender Alaveni
 * 
 * @version 1.1.0
 * 
 * @since 2018-05-17
 */
@Component
public class DynamoDBDataSourceAdapter extends DataSourceAdapter {

	@Value("${des.home}")
	private String desHome;

	private static final String JOB_NAME = "local_project.prifender_dynamodb_m3_v1_0_1.Prifender_DynamoDB_M3_v1";

	private static final String DEPENDENCY_JAR = "prifender_dynamodb_m3_v1_0_1.jar";

	private static final int MIN_THRESHOULD_ROWS = 100000;

	private static final int MAX_THRESHOULD_ROWS = 200000;

	public static final String TYPE_ID = "DynamoDB";
	public static final String TYPE_LABEL = "Amazon DynamoDB";

	// AWS access key ID

	public static final String PARAM_AWS_ACCESS_KEY_ID = "AWSAccessKey";
	public static final String PARAM_AWS_ACCESS_KEY_LABEL = "AWS access key ID";
	public static final String PARAM_AWS_ACCESS_KEY_DESCRIPTION = "The name of a AWS Access Key";

	public static final ConnectionParamDef PARAM_AWS_ACCESS_KEY = new ConnectionParamDef().id(PARAM_AWS_ACCESS_KEY_ID)
			.label(PARAM_AWS_ACCESS_KEY_LABEL).description(PARAM_AWS_ACCESS_KEY_DESCRIPTION).type(TypeEnum.STRING);

	// Secret key

	public static final String PARAM_AWS_SECRET_ACCESS_KEY_ID = "AWSSecretAccessKey";
	public static final String PARAM_AWS_SECRET_ACCESS_KEY_LABEL = "AWS Secret Access Key";
	public static final String PARAM_AWS_SECRET_ACCESS_KEY_DESCRIPTION = "The name of a AWS Secret Access Key";

	public static final ConnectionParamDef PARAM_AWS_SECRET_ACCESS_KEY = new ConnectionParamDef()
			.id(PARAM_AWS_SECRET_ACCESS_KEY_ID).label(PARAM_AWS_SECRET_ACCESS_KEY_LABEL)
			.description(PARAM_AWS_SECRET_ACCESS_KEY_DESCRIPTION).type(TypeEnum.STRING);

	// Table

	public static final String PARAM_TABLE_ID = "Table";
	public static final String PARAM_TABLE_LABEL = "Table";
	public static final String PARAM_TABLE_DESCRIPTION = "The name of the table";

	public static final ConnectionParamDef PARAM_TABLE = new ConnectionParamDef().id(PARAM_TABLE_ID)
			.label(PARAM_TABLE_LABEL).description(PARAM_TABLE_DESCRIPTION).type(TypeEnum.STRING).required(false);

	private static final DataSourceType TYPE = new DataSourceType()
			.id(TYPE_ID).label(TYPE_LABEL)
			.addConnectionParamsItem(PARAM_USER)
			.addConnectionParamsItem(PARAM_AWS_ACCESS_KEY)
			.addConnectionParamsItem(PARAM_AWS_SECRET_ACCESS_KEY)
			.addConnectionParamsItem(PARAM_TABLE);

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	/**
	 * Returns the Connection Status  based on the AmazonDynamoDB Connections
	 */
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException 
	{
		try 
		{
			AmazonDynamoDB  amazonDynamoDB = getAmazonDynamoDBClientDetails(ds);
			if (amazonDynamoDB != null) 
			{
				final String tableName = getConnectionParam(ds, PARAM_TABLE_ID);
				DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
				if (StringUtils.isNotBlank(tableName)) 
				{
					Table table = dynamoDB.getTable(tableName);

					if (table != null && StringUtils.equals(table.getTableName(), tableName)) 
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("DynamoDB connection successfully established.");
					} 
					else 
					{
						throw new IllegalArgumentException("Table '" + tableName + "' not found in datasource '" + ds.getId() + "'");
					}
				} 
				else 
				{
					if (dynamoDB.listTables() != null) 
					{
						return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("DynamoDB connection successfully established.");
					}
					else 
					{
						throw new IllegalArgumentException("No Tables are found in datasource '" + ds.getId() + "' ");
					}
				}
			}
		} 
		catch (IllegalArgumentException iae) 
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(iae.getMessage());
		} 
		catch (Exception e) 
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to DynamoDB database");
	}

	/**
	 * Gets the access key and the secret Key datasource and returns the AmazonDynamoDB 
	 * @param ds
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private AmazonDynamoDB getAmazonDynamoDBClientDetails(DataSource ds) throws DataExtractionServiceException 
	{
		if (ds == null) 
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataSource").message("datasource is null."));
		}
		final String accessKey = getConnectionParam(ds, PARAM_AWS_ACCESS_KEY_ID);
		final String secretKey = getConnectionParam(ds, PARAM_AWS_SECRET_ACCESS_KEY_ID);
		return getAmazonDynamoDBClientDetails(accessKey, secretKey);
	}

	/**
	 * Returns the AmazonDB connectionDetails  for the given accessKey and the secretKey
	 * @param accessKey
	 * @param secretKey
	 * @return AmazonDynamoDB
	 */
	@SuppressWarnings("deprecation")
	private AmazonDynamoDB getAmazonDynamoDBClientDetails(String accessKey, String secretKey) 
	{
		return new AmazonDynamoDBClient(new BasicAWSCredentials(accessKey, secretKey));
	}

	/**
	 * Gets the Metadata for the Respective table and retuns it
	 */
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException 
	{
		Metadata metadata = null;
		try 
		{
			AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDBClientDetails(ds);
			if (amazonDynamoDB != null) 
			{
				final String tableName = getConnectionParam(ds, PARAM_TABLE_ID);
				DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
				if (StringUtils.isNotBlank(tableName)) 
				{
					Table table = dynamoDB.getTable(tableName);

					if (table != null && StringUtils.equals(table.getTableName(), tableName)) 
					{
						metadata = metadataByConnection(amazonDynamoDB, ds);
					} 
					else 
					{
						throw new IllegalArgumentException("Table '" + tableName + "' not found in datasource '" + ds.getId() + "'");
					}
				} 
				else 
				{
					if (dynamoDB.listTables() != null) 
					{
						metadata =	metadataByConnection(amazonDynamoDB, ds);
					}
					else 
					{
						throw new IllegalArgumentException("tables not found in datasource '" + ds.getId() + "' ");
					}
				}
			}
		} 
		catch (IllegalArgumentException iae) 
		{
			throw new DataExtractionServiceException(new Problem().code("failure").message(iae.getMessage()));
		} 
		catch (Exception e) 
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	
	
	/**
	 * Based on the tableName returns the metadata
	 * @param dynamoDbClient
	 * @param tableName
	 * @return Metadata
	 */
	private Metadata metadataByConnection(AmazonDynamoDB amazonDynamoDB, DataSource ds) 
	{
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		List<String> tableList = null;
		final String tableName = getConnectionParam(ds, PARAM_TABLE_ID);
		if (StringUtils.isNotBlank(tableName)) 
		{
			tableList = new ArrayList<String>();
			tableList.add(tableName);
		} 
		else 
		{
			tableList = new ArrayList<String>();
			tableList.addAll(getDynamoDbClientRelatedTables(amazonDynamoDB));
		}
		for (String table : tableList) 
		{
			NamedType namedType = new NamedType();
			namedType.setName(table);
			namedType.setType(new Type().kind(Type.KindEnum.LIST));
			Type entryType = new Type().kind(Type.KindEnum.OBJECT);
			namedType.getType().setEntryType(entryType);
			List<NamedType> attributeListForColumns = getTableRelatedColumns(amazonDynamoDB, namedType.getName());
			entryType.setAttributes(attributeListForColumns);
			namedTypeObjectsList.add(namedType);
			metadata.setObjects(namedTypeObjectsList);
		}
		return metadata;
	}

	/**
	 *	Returns the tableName from dynamoDbClient
	  * @return List<String>
	 */
	private List<String> getDynamoDbClientRelatedTables(AmazonDynamoDB amazonDynamoDB) 
	{
		List<String> tableList = new ArrayList<String>();
		if (amazonDynamoDB.listTables() != null) 
		{
			tableList.addAll(amazonDynamoDB.listTables().getTableNames());
		}
		return tableList;
	}

	/**
	 * Scan the request and get the items for the respective table in dynamo db
	 * 
	 * @param dynamoDbClient
	 * @param tableName
	 * @return List<NamedType>
	 */
	private List<NamedType> getTableRelatedColumns(AmazonDynamoDB amazonDynamoDB, String tableName) 
	{
		List<NamedType> namedTypeList = new ArrayList<NamedType>();
		ScanRequest scanRequest = new ScanRequest(tableName);
		ScanResult scanResult = amazonDynamoDB.scan(scanRequest);

		if (scanResult.getItems() != null && scanResult.getItems().size() > 0) 
		{
			Map<String, AttributeValue> dynamoItem = scanResult.getItems().get(0);
			Set<String> itemKeys = dynamoItem.keySet();
			for (String item : itemKeys) 
			{
				NamedType namedType = new NamedType();
				AttributeValue attributeValue = dynamoItem.get(item);
				namedType.setName(item);
				namedType.setType(getAttributeType(attributeValue));
				namedTypeList.add(namedType);
			}
		}
		return namedTypeList;
	}

	/**
	 * Returns the Type Based on the AttributeValue
	 * @param attributeValue
	 * @return Type
	 */
	private Type getAttributeType(AttributeValue attributeValue) 
	{
		DataTypeEnum dataTypeEnum = Type.DataTypeEnum.STRING;
		if (attributeValue.getN() != null) 
		{
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		} 
		else if (attributeValue.getBOOL() != null) 
		{
			dataTypeEnum = Type.DataTypeEnum.BOOLEAN;
		} 
		else if (attributeValue.getB() != null) 
		{
			dataTypeEnum = Type.DataTypeEnum.BINARY;
		}
		return new Type().kind(Type.KindEnum.VALUE).dataType(dataTypeEnum);
	}

/**
 * Returns the number of records for a given table 
 * @param ds
 * @param tableName
 * @return
 * @throws DataExtractionServiceException
 */
	@Override
	public int getCountRows(DataSource ds, DataExtractionSpec spec) throws DataExtractionServiceException 
	{
		int count = 0;
		AmazonDynamoDB amazonDynamoDB = getAmazonDynamoDBClientDetails(ds);
		if (amazonDynamoDB != null) 
		{
			String tableName = spec.getCollection();
			DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
			if (dynamoDB.getTable(tableName).getTableName() != null) 
			{
				count  = dynamoDB.getTable(tableName).describe().getItemCount().intValue();
			}
		}
		return count;
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
			final DataExtractionThread dataExtractionExecutor = new DynamoDBDataExtractionExecutor(context, adapterHome, containersCount);
			this.threadPool.execute(dataExtractionExecutor);
			startResult = new StartResult(job, dataExtractionExecutor);
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	public class DynamoDBDataExtractionExecutor extends DataExtractionThread
	{

		private final String adapterHome;
		private final int containersCount;

		public DynamoDBDataExtractionExecutor(final DataExtractionContext context, final String adapterHome, final int containersCount) throws DataExtractionServiceException
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
			DataExtractionJob job, int objectCount, String adapterHome, int containersCount)
			throws DataExtractionServiceException 
	{
	 
		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();
		try 
		{
			  
			if (objectCount <= MIN_THRESHOULD_ROWS) 
			{
				dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, 1, objectCount));

			} 
			else 
			{
				int taskSampleSize = generateTaskSampleSize(objectCount, containersCount);

				if (taskSampleSize <= MIN_THRESHOULD_ROWS) 
				{
					taskSampleSize = MIN_THRESHOULD_ROWS;
				}
				if (taskSampleSize > MAX_THRESHOULD_ROWS) 
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

				if (remainingSampleSize > 0) 
				{
					int offset = noOfTasks * taskSampleSize + 1;
					dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, adapterHome, offset, remainingSampleSize));
				}
			}
		}
		catch (Exception e) 
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
			final String accessKey = getConnectionParam(ds, PARAM_AWS_ACCESS_KEY_ID);
			final String secretKey = getConnectionParam(ds, PARAM_AWS_SECRET_ACCESS_KEY_ID);
			String tableName = spec.getCollection();

			Map<String, String> contextParams = getContextParams(adapterHome, JOB_NAME, accessKey, secretKey, tableName.replaceAll(" ", "\\ "),

					getFormulateDataSourceColumnNames(ds, spec, ".", "@#@").replaceAll(" ", "\\ "), job.getOutputMessagingQueue(), String.valueOf(offset),

					String.valueOf(limit), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE), String.valueOf(limit));

			dataExtractionTask.taskId("DES-Task-" + getUUID())

					.jobId(job.getId())

					.typeId(TYPE_ID + "__" + JOB_NAME + "__" + DEPENDENCY_JAR)

					.contextParameters(contextParams)

					.numberOfFailedAttempts(0);
		} 
		catch (Exception e) 
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}
		return dataExtractionTask;
	}

	public Map<String, String> getContextParams(String jobFilesPath, String jobName, String accessKey,

			String secretKey, String dataSourceTableName, String dataSourceColumnNames, String jobId, String offset,

			String limit, String dataSourceScope, String dataSourceSampleSize) throws IOException 
	{

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);

		ilParamsVals.put("ACCESS_KEY", accessKey);

		ilParamsVals.put("SECRET_KEY", secretKey);

		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);

		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);

		ilParamsVals.put("JOB_ID", jobId);

		ilParamsVals.put("OFFSET", offset);

		ilParamsVals.put("LIMIT", limit);

		ilParamsVals.put("SCOPE", dataSourceScope);

		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		return ilParamsVals;

	}
  }
}
