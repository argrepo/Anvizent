package com.prifender.des.adapter.dss.googledrive;

import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.FileList;
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
public class GoogleDriveDssAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final String APPLICATION_NAME = "Prefinder Google Drive API";

	private static final int MAX_FILES_THRESHOLD = 10;

	// Service Account
	public static final String PARAM_SERVICE_ACCOUNT_ID = "ServiceAccount";
	public static final String PARAM_SERVICE_ACCOUNT_LABEL = "Service Account Name";
	public static final String PARAM_SERVICE_ACCOUNT_DESCRIPTION = "Service Account Name";

	public static final ConnectionParamDef PARAM_SERVICE_ACCOUNT = new ConnectionParamDef().id(PARAM_SERVICE_ACCOUNT_ID).label(PARAM_SERVICE_ACCOUNT_LABEL).description(PARAM_SERVICE_ACCOUNT_DESCRIPTION).type(TypeEnum.STRING);

	// Service Account Private Key String
	public static final String PARAM_SERVICE_ACCOUNT_PRIVATE_KEY_ID = "ServiceAccountPrivateKey";
	public static final String PARAM_SERVICE_PRIVATE_KEY_LABEL = "Service Account Private Key";
	public static final String PARAM_SERVICE_PRIVATE_KEY_DESCRIPTION = "Service Account Private Key";

	public static final ConnectionParamDef PARAM_SERVICE_ACCOUNT_PRIVATE_KEY = new ConnectionParamDef().id(PARAM_SERVICE_ACCOUNT_PRIVATE_KEY_ID).label(PARAM_SERVICE_PRIVATE_KEY_LABEL).description(PARAM_SERVICE_PRIVATE_KEY_DESCRIPTION).type(TypeEnum.STRING);

	public static final String TYPE_ID = "GoogleDrive";
	public static final String TYPE_LABEL = "GoogleDrive";

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_SERVICE_ACCOUNT).addConnectionParamsItem(PARAM_SERVICE_ACCOUNT_PRIVATE_KEY);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		try
		{

			getUserConnection(ds);

			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Google Drive Authorization Successful.");
		}
		catch ( DataExtractionServiceException e )
		{
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}

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

	private void getUserConnection(final DataSource ds) throws DataExtractionServiceException
	{
		try
		{
			NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
			Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getConnection(ds)).setApplicationName(APPLICATION_NAME).build();
			com.google.api.services.drive.Drive.Files.List request = service.files().list();
			request.setPageSize(1);
			request.setFields("files(id)");
			request.execute();
		}
		catch ( Exception e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}

	}

	private Credential getConnection(final DataSource ds) throws IOException, GeneralSecurityException, URISyntaxException
	{

		final String userName = getConnectionParam(ds, PARAM_USER_ID);
		final String serviceAccountName = getConnectionParam(ds, PARAM_SERVICE_ACCOUNT_ID);
		final String serviceAccountPrivateKey = getConnectionParam(ds, PARAM_SERVICE_ACCOUNT_PRIVATE_KEY_ID);

		Set<String> scopes = new HashSet<String>();
		scopes.add(DriveScopes.DRIVE);
		scopes.add(DriveScopes.DRIVE_METADATA);
		scopes.add(DriveScopes.DRIVE_FILE);

		GoogleCredential credential = new GoogleCredential.Builder().setTransport(GoogleNetHttpTransport.newTrustedTransport()).setJsonFactory(new JacksonFactory()).setServiceAccountId(serviceAccountName).setServiceAccountPrivateKey(getPrivateKey(serviceAccountPrivateKey))
				.setServiceAccountScopes(scopes).setServiceAccountUser(userName).build();

		return credential;
	}

	public PrivateKey getPrivateKey(String serviceAccountPrivateKey) throws NoSuchAlgorithmException, InvalidKeySpecException
	{

		serviceAccountPrivateKey = serviceAccountPrivateKey.replaceAll("\\n", "").replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "");
		KeyFactory kf = KeyFactory.getInstance("RSA");
		PKCS8EncodedKeySpec keySpecPKCS8 = new PKCS8EncodedKeySpec(Base64.decodeBase64(serviceAccountPrivateKey.getBytes()));
		return kf.generatePrivate(keySpecPKCS8);
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, final int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{

			String userName = getConnectionParam(ds, PARAM_USER_ID);
			userName = userName.substring(0, userName.indexOf("."));
			DataExtractionJob job = new DataExtractionJob()

					.id(spec.getDataSource() + "-" + userName + "-" + UUID.randomUUID().toString())

					.state(DataExtractionJob.StateEnum.WAITING);

			startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, containersCount));

		}
		catch ( Exception exe )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(exe.getMessage()));

		}
		return startResult;
	}

	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec,

			DataExtractionJob job, int containersCount) throws DataExtractionServiceException
	{

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

			String[] fileTypes = new String[] {
					"application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-powerpoint",
					"application/vnd.openxmlformats-officedocument.presentationml.presentation", "application/vnd.oasis.opendocument.text", "application/vnd.oasis.opendocument.spreadsheet", "application/vnd.oasis.opendocument.presentation", 
					"text/plain", "application/msword", "application/pdf", "application/rtf" };
			
			List<String> reqDocTypeList = new ArrayList<String>(Arrays.asList(fileTypes));
			NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
			Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getConnection(ds)).setApplicationName(APPLICATION_NAME).build();
			com.google.api.services.drive.Drive.Files.List request = service.files().list();
			FileList result;
			do
			{
				String fileFilterString = "not mimeType contains 'image/' and not mimeType contains 'audio/' and " + " not mimeType contains 'video/' and not mimeType contains 'photo/' and " + "mimeType != 'application/vnd.google-apps.folder' and 'me' in owners";

				request.setQ(fileFilterString);
				request.setPageSize(1000);
				request.setFields("files(id, name, mimeType, size, parents, fullFileExtension, fileExtension, originalFilename, webContentLink), nextPageToken");
				result = request.execute();

				if( result != null )
				{
					request.setPageToken(result.getNextPageToken());

					List<com.google.api.services.drive.model.File> tempFilesList = result.getFiles();

					List<FileMetaInfo> filesInfoList = new ArrayList<>(MAX_FILES_THRESHOLD);

					int index = 0;
					for (com.google.api.services.drive.model.File file : tempFilesList)
					{
						if( null != file.getWebContentLink() && null != file.getMimeType() && reqDocTypeList.contains(file.getMimeType()) )
						{
							FileMetaInfo fileMetaInfo = new FileMetaInfo().fileId(file.getId()).fileName(file.getName()).filePath(null != file.getParents() ? file.getParents().get(0) : "").fileSize(null != file.getSize() ? file.getSize().intValue() : 0).fileType(file.getMimeType())
									.fileExtension(file.getFullFileExtension()).fileDownloadLink(file.getWebContentLink());

							filesInfoList.add(fileMetaInfo);
							index++;
							objectsCount++;

							if( index % MAX_FILES_THRESHOLD == 0 )
							{
								dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, filesInfoList));
								tasksCount++;
								filesInfoList = new ArrayList<>(MAX_FILES_THRESHOLD);
							}
						}
					}

					if( (index % MAX_FILES_THRESHOLD) > 0 )
					{
						dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, filesInfoList));
						tasksCount++;
					}
				}
			}
			while (request.getPageToken() != null && request.getPageToken().length() > 0);

		}
		catch ( Exception e )
		{
			 
			throw new DataExtractionServiceException(new Problem().code("unknownDataExtractionJob").message(e.getMessage()));
		}

		synchronized (job)
		{

			job.setTasksCount(tasksCount);
			job.setObjectCount(objectsCount);

		}

		return dataExtractionJobTasks;
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

			DataExtractionJob job, List<FileMetaInfo> filesInfoList) throws DataExtractionServiceException
	{

		DataExtractionTask dataExtractionTask = new DataExtractionTask();

		try
		{

			final String userName = getConnectionParam(ds, PARAM_USER_ID);
			final String serviceAccountName = getConnectionParam(ds, PARAM_SERVICE_ACCOUNT_ID);
			final String serviceAccountPrivateKey = getConnectionParam(ds, PARAM_SERVICE_ACCOUNT_PRIVATE_KEY_ID);

			Map<String, String> contextParams = getContextParams(job.getOutputMessagingQueue(), userName, serviceAccountName, serviceAccountPrivateKey, filesInfoList, spec.getScope().name(), String.valueOf(spec.getSampleSize()));

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

	public Map<String, String> getContextParams(final String jobId, final String userName, final String serviceAccountName, final String serviceAccountPrivateKey, List<FileMetaInfo> filesInfoList, final String extractionScope, final String sampleSize) throws IOException
	{

		ObjectMapper mapperObj = new ObjectMapper();
		String filesInfo = mapperObj.writeValueAsString(filesInfoList);

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("JOB_ID", jobId);

		ilParamsVals.put("USER_NAME", userName);

		ilParamsVals.put("SERVICE_ACCOUNT_NAME", serviceAccountName);

		ilParamsVals.put("SERVICE_ACCOUNT_PRIVATE_KEY", serviceAccountPrivateKey);

		ilParamsVals.put("FILES_INFO", filesInfo);

		ilParamsVals.put("SCOPE", extractionScope);

		ilParamsVals.put("SAMPLESIZE", sampleSize);

		return ilParamsVals;

	}

}