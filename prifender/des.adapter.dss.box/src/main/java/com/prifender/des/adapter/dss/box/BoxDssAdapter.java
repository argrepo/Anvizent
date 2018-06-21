package com.prifender.des.adapter.dss.box;

import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxFolder.Info;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.IAccessTokenCache;
import com.box.sdk.InMemoryLRUAccessTokenCache;
import com.box.sdk.JWTEncryptionPreferences;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.FileMetaInfo;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;

@Component
public class BoxDssAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final int MAX_FILES_THRESHOLD = 10;

	public static final String TYPE_ID = "Box";
	public static final String TYPE_LABEL = "Box";

	// PUBLIC_KEY
	public static final String PARAM_PUBLIC_KEY_ID = "PublicKey";
	public static final String PARAM_PUBLIC_KEY_LABEL = "PublicKey";
	public static final String PARAM_PUBLIC_KEY_DESCRIPTION = "PublicKey, which is required to get JWTEncryptionPreferences";

	public static final ConnectionParamDef PARAM_PUBLIC_KEY = new ConnectionParamDef().id(PARAM_PUBLIC_KEY_ID).label(PARAM_PUBLIC_KEY_LABEL).description(PARAM_PUBLIC_KEY_DESCRIPTION).type(TypeEnum.STRING);

	// PrivateKeyPassword
	public static final String PARAM_PRIVATE_KEY_PASSWORD_ID = "PrivateKeyPassword";
	public static final String PARAM_PRIVATE_KEY_PASSWORD_LABEL = "PrivateKeyPassword";
	public static final String PARAM_PRIVATE_KEY_PASSWORD_DESCRIPTION = "PrivateKeyPassword, which is required to get JWTEncryptionPreferences";

	public static final ConnectionParamDef PARAM_PRIVATE_KEY_PASSWORD = new ConnectionParamDef().id(PARAM_PRIVATE_KEY_PASSWORD_ID).label(PARAM_PRIVATE_KEY_PASSWORD_LABEL).description(PARAM_PRIVATE_KEY_PASSWORD_DESCRIPTION).type(TypeEnum.STRING);

	// PrivateKey
	public static final String PARAM_PRIVATE_KEY_ID = "PrivateKey";
	public static final String PARAM_PRIVATE_KEY_LABEL = "PrivateKey";
	public static final String PARAM_PRIVATE_KEY_DESCRIPTION = "PrivateKey, which is required to get JWTEncryptionPreferences";

	public static final ConnectionParamDef PARAM_PRIVATE_KEY = new ConnectionParamDef().id(PARAM_PRIVATE_KEY_ID).label(PARAM_PRIVATE_KEY_LABEL).description(PARAM_PRIVATE_KEY_DESCRIPTION).type(TypeEnum.STRING);

	// ClientID
	public static final String PARAM_CLIENT_ID = "Client";
	public static final String PARAM_CLIENT_ID_LABEL = "Client";
	public static final String PARAM_CLIENT_ID_DESCRIPTION = "Client, which is required to get Box Config";

	public static final ConnectionParamDef PARAM_CLIENTID = new ConnectionParamDef().id(PARAM_CLIENT_ID).label(PARAM_CLIENT_ID_LABEL).description(PARAM_CLIENT_ID_DESCRIPTION).type(TypeEnum.STRING);

	// Client Secret
	public static final String PARAM_CLIENT_SECRET_ID = "ClientSecret";
	public static final String PARAM_CLIENT_SECRET_LABEL = "ClientSecret";
	public static final String PARAM_CLIENT_SECRET_DESCRIPTION = "ClientSecret, which is required to get Box Config";

	public static final ConnectionParamDef PARAM_CLIENT_SECRET = new ConnectionParamDef().id(PARAM_CLIENT_SECRET_ID).label(PARAM_CLIENT_SECRET_LABEL).description(PARAM_CLIENT_SECRET_DESCRIPTION).type(TypeEnum.STRING);

	// Enterprise ID
	public static final String PARAM_ENTERPRISE_ID = "Enterprise";
	public static final String PARAM_ENTERPRISE_ID_LABEL = "Enterprise";
	public static final String PARAM_ENTERPRISE_ID_DESCRIPTION = "Enterprise, which is required to get Box Config";

	public static final ConnectionParamDef PARAM_ENTERPRISEID = new ConnectionParamDef().id(PARAM_ENTERPRISE_ID).label(PARAM_ENTERPRISE_ID_LABEL).description(PARAM_ENTERPRISE_ID_DESCRIPTION).type(TypeEnum.STRING);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_PUBLIC_KEY).addConnectionParamsItem(PARAM_PRIVATE_KEY_PASSWORD).addConnectionParamsItem(PARAM_PRIVATE_KEY)
			.addConnectionParamsItem(PARAM_CLIENTID).addConnectionParamsItem(PARAM_CLIENT_SECRET).addConnectionParamsItem(PARAM_ENTERPRISEID);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		BoxDeveloperEditionAPIConnection api = null;

		try
		{
			api = getUserConnection(ds);
			if( api != null )
			{
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Box Authorization Successful.");
			}
		}
		catch ( IOException | GeneralSecurityException | URISyntaxException e )
		{
			e.printStackTrace();
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Box Authorization Failed.");
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;

		try
		{
			metadata = getDSSMetadata(ds);
		}
		catch ( IOException | GeneralSecurityException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	private Metadata getDSSMetadata(DataSource ds) throws IOException, GeneralSecurityException
	{
		Metadata metadata = new Metadata();
		List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();

		List<NamedType> attributeListForColumns = new ArrayList<NamedType>();

		List<String> metaDataProps = Arrays.asList("Path", "Type", "Size", "ChunkNumber", "Content");

		NamedType pathNamedType = new NamedType();
		pathNamedType.setName(metaDataProps.get(0));
		Type type1 = new Type().kind(Type.KindEnum.VALUE).dataType(Type.DataTypeEnum.STRING);
		pathNamedType.setType(type1);
		attributeListForColumns.add(pathNamedType);

		NamedType typeNamedType = new NamedType();
		typeNamedType.setName(metaDataProps.get(1));
		Type type2 = new Type().kind(Type.KindEnum.VALUE).dataType(Type.DataTypeEnum.STRING);
		typeNamedType.setType(type2);
		attributeListForColumns.add(typeNamedType);

		NamedType sizeNamedType = new NamedType();
		sizeNamedType.setName(metaDataProps.get(2));
		Type type3 = new Type().kind(Type.KindEnum.VALUE).dataType(Type.DataTypeEnum.INTEGER);
		sizeNamedType.setType(type3);
		attributeListForColumns.add(sizeNamedType);

		NamedType chunkNoNamedType = new NamedType();
		chunkNoNamedType.setName(metaDataProps.get(3));
		Type type4 = new Type().kind(Type.KindEnum.VALUE).dataType(Type.DataTypeEnum.INTEGER);
		chunkNoNamedType.setType(type4);
		attributeListForColumns.add(chunkNoNamedType);

		NamedType contentNamedType = new NamedType();
		contentNamedType.setName(metaDataProps.get(4));
		Type type5 = new Type().kind(Type.KindEnum.VALUE).dataType(Type.DataTypeEnum.STRING);
		contentNamedType.setType(type5);
		attributeListForColumns.add(contentNamedType);

		NamedType namedType = new NamedType();
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		Type entryType = new Type().kind(Type.KindEnum.OBJECT);
		namedType.getType().setEntryType(entryType);

		namedType.setName("Files");
		entryType.setAttributes(attributeListForColumns);
		namedTypeObjectsList.add(namedType);
		metadata.setObjects(namedTypeObjectsList);

		return metadata;
	}

	private BoxDeveloperEditionAPIConnection getConnection(final DataSource ds) throws IOException, GeneralSecurityException, URISyntaxException
	{
		int MAX_CACHE_ENTRIES = 100;
		IAccessTokenCache accessTokenCache = new InMemoryLRUAccessTokenCache(MAX_CACHE_ENTRIES);
		BoxDeveloperEditionAPIConnection api = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(getBoxConfig(ds), accessTokenCache);

		return api;
	}

	private BoxDeveloperEditionAPIConnection getUserConnection(final DataSource ds) throws IOException, GeneralSecurityException, URISyntaxException
	{
		final String userName = getConnectionParam(ds, PARAM_USER_ID);

		Iterable<BoxUser.Info> users = BoxUser.getAllEnterpriseUsers(getConnection(ds), userName, "login", "id", "name");
		Iterator<BoxUser.Info> iterator = users.iterator();

		/*
		 * While Loop Runs Only Once, since "users" will contain only one object
		 */
		while (iterator.hasNext())
		{
			BoxUser.Info boxUserInfo = (BoxUser.Info) iterator.next();
			String userId = boxUserInfo.getID();
			BoxDeveloperEditionAPIConnection apiConn = BoxDeveloperEditionAPIConnection.getAppUserConnection(userId, getBoxConfig(ds));

			return apiConn;
		}

		return null;
	}

	private BoxConfig getBoxConfig(final DataSource ds)
	{
		final String publicId = getConnectionParam(ds, PARAM_PUBLIC_KEY_ID);
		final String privateKeyPwd = getConnectionParam(ds, PARAM_PRIVATE_KEY_PASSWORD_ID);
		final String privateKey = getConnectionParam(ds, PARAM_PRIVATE_KEY_ID);
		final String clientId = getConnectionParam(ds, PARAM_CLIENT_ID);
		final String clientSecret = getConnectionParam(ds, PARAM_CLIENT_SECRET_ID);
		final String enterpriseId = getConnectionParam(ds, PARAM_ENTERPRISE_ID);

		JWTEncryptionPreferences jwtPreferences = new JWTEncryptionPreferences();
		jwtPreferences.setPublicKeyID(publicId);
		jwtPreferences.setPrivateKeyPassword(privateKeyPwd);
		jwtPreferences.setPrivateKey(privateKey);
		jwtPreferences.setEncryptionAlgorithm(EncryptionAlgorithm.RSA_SHA_256);

		return new BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences);
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, final int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final String clientId = getConnectionParam(ds, PARAM_CLIENT_ID);
			
			DataExtractionJob job = new DataExtractionJob()

					.id(spec.getDataSource() + "-" + clientId + "-" + UUID.randomUUID().toString())

					.state(DataExtractionJob.StateEnum.WAITING);

			startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, containersCount));
		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("job error").message(exe.getMessage()));

		}
		return startResult;
	}

	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, int containersCount) throws DataExtractionServiceException
	{

		final String userName = getConnectionParam(ds, PARAM_USER_ID);

		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();

		int objectsCount = 0;
		int tasksCount = 0;

		try
		{
			synchronized (job)
			{

				job.setOutputMessagingQueue("DES-" + job.getId());

				job.objectsExtracted(0);

				job.setTasksCount(tasksCount);

				job.setObjectCount(objectsCount);

			}

			Iterable<BoxUser.Info> users = BoxUser.getAllEnterpriseUsers(getConnection(ds), userName, "login", "id", "name");
			Iterator<BoxUser.Info> iterator = users.iterator();

			/*
			 * While Loop Runs Only Once, since "users" will contain only one
			 * object
			 */
			while (iterator.hasNext())
			{
				BoxUser.Info boxUserInfo = (BoxUser.Info) iterator.next();
				String userId = boxUserInfo.getID();
				BoxDeveloperEditionAPIConnection apiConn = BoxDeveloperEditionAPIConnection.getAppUserConnection(userId, getBoxConfig(ds));

				BoxFolder rootFolder = BoxFolder.getRootFolder(apiConn);

				List<FileMetaInfo> filesInfoList = new ArrayList<>();

				List<String> folderIds = new ArrayList<>();
				listFolder(rootFolder, apiConn, folderIds, "out");

				for (String folderId : folderIds)
				{
					BoxFolder folder = new BoxFolder(apiConn, folderId);
					scanFilesInFolder(folder, filesInfoList, apiConn);
				}

				objectsCount = filesInfoList.size();

				for (int i = 0; i < filesInfoList.size(); i += 10)
				{
					List<FileMetaInfo> tmpFilesInfoList = new ArrayList<>(MAX_FILES_THRESHOLD);
					int start = i;
					int end = (i + 10);
					if( start >= objectsCount )
					{
						start = objectsCount;
					}
					if( end > objectsCount )
					{
						end = objectsCount;
					}
					tmpFilesInfoList = filesInfoList.subList(start, end);

					dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, tmpFilesInfoList, userId));
					tasksCount++;
				}
			}
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}

		if( objectsCount == 0 )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message("No Files to process!!!!"));
		}

		synchronized (job)
		{
			job.setTasksCount(tasksCount);
			job.setObjectCount(objectsCount);
		}

		return dataExtractionJobTasks;
	}

	private void listFolder(BoxFolder folder, BoxDeveloperEditionAPIConnection apiConn, List<String> folderIds, String callFrom)
	{
		if( "out".equals(callFrom) )
		{
			folderIds.add(folder.getID());
		}
		for (BoxItem.Info itemInfo : folder)
		{
			if( itemInfo instanceof BoxFolder.Info )
			{
				BoxFolder childFolder = (BoxFolder) itemInfo.getResource();
				folderIds.add(childFolder.getID());
				listFolder(childFolder, apiConn, folderIds, "in");
			}
		}
	}

	private void scanFilesInFolder(BoxFolder folder, List<FileMetaInfo> filesInfoList, BoxDeveloperEditionAPIConnection apiConn)
	{
		String[] fileTypes = new String[] { "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "txt", "rtf", "pdf" };

		List<String> reqDocTypeList = new ArrayList<String>(Arrays.asList(fileTypes));

		for (BoxItem.Info itemInfo : folder)
		{
			BoxFile file = new BoxFile(apiConn, itemInfo.getID());
			BoxFile.Info fileInfo = null;
			try
			{
				fileInfo = file.getInfo();
			}
			catch ( Exception e )
			{
				continue;
			}

			String fileExtension = "";

			if( null != fileInfo && null != fileInfo.getName() )
			{
				if( null == fileInfo.getExtension() )
				{
					fileExtension = fileInfo.getName().substring(fileInfo.getName().lastIndexOf(".") + 1);
				}
				else
				{
					fileExtension = fileInfo.getExtension();
				}
			}

			if( null != fileInfo && null != fileInfo.getName() && fileInfo.getName().indexOf("managed_users") == -1 && reqDocTypeList.contains(fileExtension) && null != fileInfo.getID() )
			{
				String filePath = getFilePath(fileInfo.getPathCollection());

				if( filePath.length() == 0 )
				{
					filePath = fileInfo.getParent().getName();
				}

				FileMetaInfo fileMetaInfo = new FileMetaInfo().fileId(fileInfo.getID()).fileName(fileInfo.getName()).filePath(filePath).fileSize((int) fileInfo.getSize()).fileType(fileInfo.getExtension()).fileExtension(fileInfo.getExtension());

				filesInfoList.add(fileMetaInfo);
			}
		}
	}

	private String getFilePath(List<Info> pathCollection)
	{
		String filePath = "";
		Iterator<Info> paths = pathCollection.iterator();

		while (paths.hasNext())
		{
			filePath += "/" + paths.next().getName();
		}

		return filePath;
	}

	/**
	 * 
	 * @param ds
	 * @param spec
	 * @param job
	 * @param fileId
	 * @param fileType
	 * @param fileSize
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, List<FileMetaInfo> filesInfoList, String userId) throws DataExtractionServiceException
	{

		DataExtractionTask dataExtractionTask = new DataExtractionTask();

		try
		{

			final String publicId = getConnectionParam(ds, PARAM_PUBLIC_KEY_ID);
			final String privateKeyPwd = getConnectionParam(ds, PARAM_PRIVATE_KEY_PASSWORD_ID);
			final String privateKey = getConnectionParam(ds, PARAM_PRIVATE_KEY_ID);
			final String clientId = getConnectionParam(ds, PARAM_CLIENT_ID);
			final String clientSecret = getConnectionParam(ds, PARAM_CLIENT_SECRET_ID);
			final String enterpriseId = getConnectionParam(ds, PARAM_ENTERPRISE_ID);

			Map<String, String> contextParams = getContextParams(job.getOutputMessagingQueue(), userId, publicId, privateKeyPwd, privateKey, clientId, clientSecret, enterpriseId, filesInfoList, spec.getScope().name(), String.valueOf(spec.getSampleSize()));

			dataExtractionTask.taskId("DES-Task-" + getUUID())

					.jobId(job.getId())

					.typeId(TYPE_ID)

					.contextParameters(contextParams)

					.numberOfFailedAttempts(0);
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}
		return dataExtractionTask;
	}

	private Map<String, String> getContextParams(String jobId, String userId, String publicId, String privateKeyPwd, String privateKey, String clientId, String clientSecret, String enterpriseId, List<FileMetaInfo> filesInfoList, final String extractionScope, final String sampleSize)
			throws IOException
	{

		ObjectMapper mapperObj = new ObjectMapper();
		String filesInfo = mapperObj.writeValueAsString(filesInfoList);

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("USER_ID", userId);

		ilParamsVals.put("PUBLIC_ID", publicId);

		ilParamsVals.put("PRIVATE_KEY_PWD", privateKeyPwd);

		ilParamsVals.put("PRIVATE_KEY", privateKey);

		ilParamsVals.put("CLIENT_ID", clientId);

		ilParamsVals.put("CLIENT_SECRET", clientSecret);

		ilParamsVals.put("ENTERPRISE_ID", enterpriseId);

		ilParamsVals.put("FILES_INFO", filesInfo);

		ilParamsVals.put("SCOPE", extractionScope);

		ilParamsVals.put("SAMPLESIZE", sampleSize);

		return ilParamsVals;

	}

}