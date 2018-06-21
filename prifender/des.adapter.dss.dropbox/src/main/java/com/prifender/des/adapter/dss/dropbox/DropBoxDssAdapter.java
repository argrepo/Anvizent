package com.prifender.des.adapter.dss.dropbox;

import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getUUID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.dropbox.core.DbxApiException;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.DbxTeamClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderBuilder;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.team.GroupMemberInfo;
import com.dropbox.core.v2.team.GroupSelector;
import com.dropbox.core.v2.team.GroupsListResult;
import com.dropbox.core.v2.team.GroupsMembersListResult;
import com.dropbox.core.v2.team.MemberProfile;
import com.dropbox.core.v2.teamcommon.GroupSummary;
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
public class DropBoxDssAdapter extends DataSourceAdapter
{

	@Value("${des.home}")
	private String desHome;

	private static final int MAX_FILES_THRESHOLD = 10;

	public static final String TYPE_ID = "Dropbox";
	public static final String TYPE_LABEL = "Dropbox";

	// DropBox API Token
	public static final String PARAM_ACCESS_TOKEN_ID = "AccessToken";
	public static final String PARAM_ACCESS_TOKEN_LABEL = "Access Token";
	public static final String PARAM_ACCESS_TOKEN_DESCRIPTION = "AccessToken, which is required for authorization";

	public static final ConnectionParamDef PARAM_ACCESS_TOKEN = new ConnectionParamDef().id(PARAM_ACCESS_TOKEN_ID).label(PARAM_ACCESS_TOKEN_LABEL).description(PARAM_ACCESS_TOKEN_DESCRIPTION).type(TypeEnum.STRING);

	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL).addConnectionParamsItem(PARAM_USER).addConnectionParamsItem(PARAM_ACCESS_TOKEN);

	@Override
	public DataSourceType getDataSourceType()
	{
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException
	{
		DbxTeamClientV2 teamClient = null;

		try
		{
			teamClient = getConnection(ds);
			if( teamClient != null )
			{
				final String userName = getConnectionParam(ds, PARAM_USER_ID);

				String memberId = getMemberId(teamClient, userName);

				if( null != memberId )
				{
					return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("DropBox Authorization Successful.");
				}
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		}

		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("DropBox Authorization Failed.");
	}

	private DbxTeamClientV2 getConnection(final DataSource ds)
	{
		final String accessToken = getConnectionParam(ds, PARAM_ACCESS_TOKEN_ID);
		DbxRequestConfig config = new DbxRequestConfig("Prefinder-Demo");
		return new DbxTeamClientV2(config, accessToken);
	}

	private String getMemberId(DbxTeamClientV2 teamClient, String accountName) throws DbxApiException, DbxException
	{
		GroupsListResult groups = teamClient.team().groupsList();
		List<GroupSummary> groupInfo = groups.getGroups();

		List<String> groupIds = new ArrayList<>();
		for (GroupSummary groupSummary : groupInfo)
		{
			groupIds.add(groupSummary.getGroupId());
		}

		for (String groupId : groupIds)
		{
			GroupSelector selector = GroupSelector.groupId(groupId);
			GroupsMembersListResult groupMems = teamClient.team().groupsMembersList(selector);

			List<GroupMemberInfo> memberInfoList = groupMems.getMembers();
			for (GroupMemberInfo memberInfo : memberInfoList)
			{
				MemberProfile memberProfile = memberInfo.getProfile();
				if( accountName.equals(memberProfile.getEmail()) )
				{
					return memberProfile.getTeamMemberId();
				}
			}
		}
		return null;
	}

	private DbxClientV2 getUserAccount(DbxTeamClientV2 teamClient, String memberId)
	{
		DbxClientV2 userClient = teamClient.asMember(memberId);
		return userClient;
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException
	{
		Metadata metadata = null;

		try
		{
			metadata = getDSSMetadata(ds);
		}
		catch ( IOException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		return metadata;
	}

	private Metadata getDSSMetadata(final DataSource ds) throws IOException
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

	public Map<String, Object> getFilesInfoMap(final DataSource ds) throws DataExtractionServiceException
	{
		Map<String, Object> infoMap = new HashMap<>();

		List<FileMetaInfo> filesInfoList = new ArrayList<>();
		try
		{
			String[] fileTypes = new String[] { "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "txt", "rtf", "pdf" };

			List<String> reqDocTypeList = new ArrayList<String>(Arrays.asList(fileTypes));

			DbxTeamClientV2 teamClient = getConnection(ds);
			if( teamClient != null )
			{
				final String userName = getConnectionParam(ds, PARAM_USER_ID);

				String memberId = getMemberId(teamClient, userName);
				infoMap.put("memberId", memberId);
				DbxClientV2 userClient = getUserAccount(teamClient, memberId);

				if( null != userClient )
				{
					ListFolderBuilder listFolderBuilder = userClient.files().listFolderBuilder("");
					ListFolderResult result = listFolderBuilder.withRecursive(true).start();
 
					while (true)
					{
						for (com.dropbox.core.v2.files.Metadata metadata : result.getEntries())
						{
							if( metadata instanceof FileMetadata )
							{

								String fileExtension = "";

								if( null != metadata && null != metadata.getName() )
								{
									fileExtension = metadata.getName().substring(metadata.getName().lastIndexOf(".") + 1);
								}

								if( null != metadata && reqDocTypeList.contains(fileExtension) )
								{
									FileMetaInfo fileMetaInfo = new FileMetaInfo().fileId(((FileMetadata) metadata).getId()).fileName(metadata.getName()).filePath(metadata.getPathDisplay()).fileExtension(fileExtension).fileSize((int) ((FileMetadata) metadata).getSize())
											.fileType(fileExtension.toUpperCase());

									filesInfoList.add(fileMetaInfo);
								}
							}
						}
						if( !result.getHasMore() )
						{
							break;
						}
						result = userClient.files().listFolderContinue(result.getCursor());
					}
				}
			}
		}
		catch ( DbxException e )
		{
			throw new DataExtractionServiceException(new Problem().code("unknownConnection").message(e.getMessage()));
		}
		infoMap.put("filesInfoList", filesInfoList);
		return infoMap;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec, final int containersCount) throws DataExtractionServiceException
	{
		StartResult startResult = null;
		try
		{
			final String userName = getConnectionParam(ds, PARAM_USER_ID);
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

			Map<String, Object> infoMap = getFilesInfoMap(ds);
			String memberId = (String) infoMap.get("memberId");
			@SuppressWarnings("unchecked")
			List<FileMetaInfo> filesInfoList = (List<FileMetaInfo>) infoMap.get("filesInfoList");

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

				dataExtractionJobTasks.add(getDataExtractionTask(ds, spec, job, memberId, tmpFilesInfoList));
				tasksCount++;
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

			DataExtractionJob job, String memberId, List<FileMetaInfo> filesInfoList) throws DataExtractionServiceException
	{

		DataExtractionTask dataExtractionTask = new DataExtractionTask();

		try
		{

			final String userName = getConnectionParam(ds, PARAM_USER_ID);
			final String accessToken = getConnectionParam(ds, PARAM_ACCESS_TOKEN_ID);

			Map<String, String> contextParams = getContextParams(job.getOutputMessagingQueue(), userName, memberId, accessToken, filesInfoList, spec.getScope().name(), String.valueOf(spec.getSampleSize()));

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

	private Map<String, String> getContextParams(String jobId, String userName, String memberId, String accessToken, List<FileMetaInfo> filesInfoList, final String extractionScope, final String sampleSize) throws IOException
	{

		ObjectMapper mapperObj = new ObjectMapper();
		
		String filesInfo = mapperObj.writeValueAsString(filesInfoList);

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("USER_NAME", userName);

		ilParamsVals.put("MEMBER_ID", memberId);

		ilParamsVals.put("ACCESS_TOKEN", accessToken);

		ilParamsVals.put("FILES_INFO", filesInfo);

		ilParamsVals.put("SCOPE", extractionScope);

		ilParamsVals.put("SAMPLESIZE", sampleSize);

		return ilParamsVals;

	}

}