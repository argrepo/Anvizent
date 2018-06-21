package com.prifender.des.node;

import static com.prifender.des.node.DocExtractionUtil.createDir;
import static com.prifender.des.node.DocExtractionUtil.deleteFiles;
import static com.prifender.des.node.DocExtractionUtil.splitStringToChunks;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.JWTEncryptionPreferences;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prifender.des.model.DocExtractionResult;
import com.prifender.des.model.FileMetaInfo;

public class BoxDocExtractionJobExecution {

	private static final int DEFAULT_CHUNK_SIZE = 5 * 1024;
	
	public List<DocExtractionResult> fetchAndExtractDocContent(Map<String, String> contextParams) throws Exception 
	{
		
		ObjectMapper mapper = new ObjectMapper();
		
		List<FileMetaInfo> filesInfoList = mapper.readValue(contextParams.get("FILES_INFO"), new TypeReference<List<FileMetaInfo>>() { });
		
		//final String userName = (String) contextParams.get("USER_NAME");
		final String publicId = (String) contextParams.get("PUBLIC_ID");
		final String privateKeyPwd = (String) contextParams.get("PRIVATE_KEY_PWD");
		final String privateKey = (String) contextParams.get("PRIVATE_KEY");
		final String clientId = (String) contextParams.get("CLIENT_ID");
		final String clientSecret = (String) contextParams.get("CLIENT_SECRET");
		final String enterpriseId = (String) contextParams.get("ENTERPRISE_ID");
		final String userId = (String) contextParams.get("USER_ID");
		
		String scope = (String) contextParams.get("SCOPE");
		String sampleSize = (String) contextParams.get("SAMPLESIZE");

		String downloadFilePath = createDir(UUID.randomUUID().toString());
		String tempPath = downloadFilePath;
		List<DocExtractionResult> resultList = new ArrayList<>();
		try {
			
			BoxDeveloperEditionAPIConnection apiUserConn = getAppUserConnection(userId, publicId, privateKeyPwd, privateKey, clientId, clientSecret, enterpriseId);
			for(FileMetaInfo fileMetaInfo : filesInfoList) 
			{
				String docContent = getFileContent(apiUserConn, fileMetaInfo, userId, downloadFilePath);
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
	
	private String getFileContent(BoxDeveloperEditionAPIConnection apiUserConn, FileMetaInfo fileMetaInfo, String userId, String downloadFilePath) 
	{

		String fileText = "";
		String filePath = null;
		try {
			filePath = downloadFile(apiUserConn, fileMetaInfo, userId, downloadFilePath);
		} catch (IOException e) 
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

	private String downloadFile(BoxDeveloperEditionAPIConnection apiUserConn, FileMetaInfo fileMetaInfo, String userId, String downloadFilePath) throws IOException
	{
		String filePath = "";
		OutputStream os = null;
		try {
			filePath = downloadFilePath + "/" + fileMetaInfo.getFileName();
			os = new BufferedOutputStream(new FileOutputStream(filePath));
			BoxFile file = new BoxFile(apiUserConn, fileMetaInfo.getFileId());
			file.download(os);
		} catch (IOException e) 
		{
			e.printStackTrace();
			throw e;
		} finally {
			if(null != os)
			{
				os.flush();
				os.close();
			}
		}
		
		return filePath;
	}

	private BoxDeveloperEditionAPIConnection getAppUserConnection(final String userId, final String publicId, final String privateKeyPwd, 
			final String privateKey, final String clientId, final String clientSecret, final String enterpriseId) 
	{
		return BoxDeveloperEditionAPIConnection.getAppUserConnection(userId, getBoxConfig(publicId, privateKeyPwd, privateKey, clientId, clientSecret, enterpriseId));
	}
	
	private BoxConfig getBoxConfig(final String publicId, final String privateKeyPwd, 
			final String privateKey, final String clientId, final String clientSecret, final String enterpriseId) 
	{
		JWTEncryptionPreferences jwtPreferences = new JWTEncryptionPreferences();
		jwtPreferences.setPublicKeyID(publicId);
		jwtPreferences.setPrivateKeyPassword(privateKeyPwd);
		jwtPreferences.setPrivateKey(privateKey);
		jwtPreferences.setEncryptionAlgorithm(EncryptionAlgorithm.RSA_SHA_256);

		return new BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences);
	}

}