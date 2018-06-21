package com.prifender.des.node;

import static com.prifender.des.node.DocExtractionUtil.createDir;
import static com.prifender.des.node.DocExtractionUtil.deleteFiles;
import static com.prifender.des.node.DocExtractionUtil.splitStringToChunks;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.DbxTeamClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.model.DocExtractionResult;
import com.prifender.des.model.FileMetaInfo;

public class DropBoxDocExtractionJobExecution {

private static final int DEFAULT_CHUNK_SIZE = 5 * 1024;
	
	public List<DocExtractionResult> fetchAndExtractDocContent(Map<String, String> contextParams) throws Exception 
	{
		ObjectMapper mapper = new ObjectMapper();
		
		List<FileMetaInfo> filesInfoList = mapper.readValue(contextParams.get("FILES_INFO"), new TypeReference<List<FileMetaInfo>>() { });
		
		//final String userName = (String) contextParams.get("USER_NAME");
		final String accessToken = (String) contextParams.get("ACCESS_TOKEN");
		final String memberId = (String) contextParams.get("MEMBER_ID");
		
		String scope = (String) contextParams.get("SCOPE");
		String sampleSize = (String) contextParams.get("SAMPLESIZE");

		String downloadFilePath = createDir(UUID.randomUUID().toString());
		String tempPath = downloadFilePath;
		List<DocExtractionResult> resultList = new ArrayList<>();
		try {
			
			DbxClientV2 userClient = getUserAccount(accessToken, memberId);
			for(FileMetaInfo fileMetaInfo : filesInfoList) 
			{
				String docContent = getFileContent(userClient, fileMetaInfo, downloadFilePath);
				if(StringUtils.isNotBlank(docContent)) 
				{
					List<String> chunksList = splitStringToChunks(docContent, DEFAULT_CHUNK_SIZE);
					
					int i = 1;
					for(String chunk: chunksList) 
					{
						DocExtractionResult docExtractionResult = new DocExtractionResult();
						
						//docExtractionResult.fileId(fileMetaInfo.getFileId());
						docExtractionResult.fileName(fileMetaInfo.getFileName());
						docExtractionResult.filePath(fileMetaInfo.getFilePath());
						docExtractionResult.fileSize(fileMetaInfo.getFileSize());
						docExtractionResult.fileType(fileMetaInfo.getFileType());
						docExtractionResult.fileContent(chunk);
						docExtractionResult.chunkNumber(i);
						i++;
						resultList.add(docExtractionResult);
					}
					
					if(StringUtils.isNotBlank(scope) && "sample".equals(scope) && resultList.size() >= Integer.valueOf(sampleSize)) 
					{
						resultList = resultList.subList(0, Integer.valueOf(sampleSize));
						break;
					}
				}
			}
		} catch (IOException e) 
		{
			e.printStackTrace();
			throw new Exception( e.getMessage() );
		} finally 
		{
			deleteFiles(tempPath);
		}
		return resultList;
	}
	
	private String getFileContent(DbxClientV2 userClient, FileMetaInfo fileMetaInfo, String downloadFilePath) 
	{

		String fileText = "";
		String filePath = null;
		try {
			filePath = downloadFile(userClient, fileMetaInfo, downloadFilePath);
		} catch (IOException | DbxException e) 
		{
			System.out.println("Error While Downloading the file");
			e.printStackTrace();
		}
		
		File docFile = new File(filePath);
		
		if(docFile.exists() && docFile.length() > 0) 
		{
			try {
				fileText = DocExtractionUtil.extractFileText(docFile);
			} catch (IOException | SAXException | TikaException e) 
			{
				System.out.println("Error While Extracting the content");
				e.printStackTrace();
			}
		}
		
		return fileText;
	}

	private String downloadFile(DbxClientV2 userClient, FileMetaInfo fileMetaInfo, String downloadFilePath) throws IOException, DbxException
	{
		String filePath = "";
		OutputStream os = null;
		InputStream is = null;
		try {
			filePath = downloadFilePath + "/" + fileMetaInfo.getFileName();
			os = new BufferedOutputStream(new FileOutputStream(filePath));
			
			//FileMetadata metadata = userClient.files().downloadBuilder(fileMetaInfo.getFilePath()).download(stream);
			//userClient.files().downloadBuilder(fileMetaInfo.getFilePath()).download(stream);
			DbxDownloader<FileMetadata> dbxDownloader = userClient.files().downloadBuilder(fileMetaInfo.getFilePath()).start();
			
			is = dbxDownloader.getInputStream();
			
			byte[] buffer = new byte[1024];
	        int length;
	        while ((length = is.read(buffer)) > 0) {
	            os.write(buffer, 0, length);
	        }
		} catch (IOException | DbxException e) 
		{
			e.printStackTrace();
			throw e;
		} finally 
		{
			if(null != is)
			{
				is.close();
			}
			if(null != os)
			{
				os.flush();
				os.close();
			}
		}
		
		return filePath;
	}

	private DbxClientV2 getUserAccount(final String accessToken, final String memberId)
	{
		DbxRequestConfig config = new DbxRequestConfig("Prefinder-Demo");
		DbxTeamClientV2 teamClient = new DbxTeamClientV2(config, accessToken);
		DbxClientV2 userClient = teamClient.asMember(memberId);
		
		return userClient;
	}
	
}