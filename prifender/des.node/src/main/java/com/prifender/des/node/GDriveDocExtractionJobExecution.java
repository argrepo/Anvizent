package com.prifender.des.node;

import static com.prifender.des.node.DocExtractionUtil.createDir;
import static com.prifender.des.node.DocExtractionUtil.deleteFiles;
import static com.prifender.des.node.DocExtractionUtil.splitStringToChunks;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.prifender.des.model.DocExtractionResult;
import com.prifender.des.model.FileMetaInfo;

public class GDriveDocExtractionJobExecution
{

	private static final String APPLICATION_NAME = "Prefinder Google Drive API";

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static final int DEFAULT_CHUNK_SIZE = 5 * 1024;

	public List<DocExtractionResult> fetchAndExtractDocContent(Map<String, String> contextParams) throws Exception
	{

		ObjectMapper mapper = new ObjectMapper();

		List<FileMetaInfo> filesInfoList = mapper.readValue(contextParams.get("FILES_INFO"), new TypeReference<List<FileMetaInfo>>()
		{
		});

		final String userName = (String) contextParams.get("USER_NAME");
		final String serviceAccountName = (String) contextParams.get("SERVICE_ACCOUNT_NAME");
		final String serviceAccountPrivateKey = (String) contextParams.get("SERVICE_ACCOUNT_PRIVATE_KEY");

		String scope = (String) contextParams.get("SCOPE");
		String sampleSize = (String) contextParams.get("SAMPLESIZE");

		String downloadFilePath = createDir(UUID.randomUUID().toString());

		List<DocExtractionResult> resultList = new ArrayList<>();
		try
		{
			NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getConnection(userName, serviceAccountName, serviceAccountPrivateKey)).setApplicationName(APPLICATION_NAME).build();

			Map<String, String> folderMetaInfo = getFoldersMetaInfo(service);

			for (FileMetaInfo fileMetaInfo : filesInfoList)
			{

				String docContent = getFileContent(service, fileMetaInfo, downloadFilePath);
				if( StringUtils.isNotBlank(docContent) )
				{
					String filePath = getFilePath(service, fileMetaInfo.getFilePath(), folderMetaInfo);
					List<String> chunksList = splitStringToChunks(docContent, DEFAULT_CHUNK_SIZE);

					int i = 1;
					for (String chunk : chunksList)
					{
						DocExtractionResult docExtractionResult = new DocExtractionResult();
						docExtractionResult.fileName(fileMetaInfo.getFileName());
						docExtractionResult.filePath(filePath);
						docExtractionResult.fileSize(fileMetaInfo.getFileSize());
						docExtractionResult.fileType(fileMetaInfo.getFileType());
						docExtractionResult.fileContent(chunk);
						docExtractionResult.chunkNumber(i);
						i++;
						resultList.add(docExtractionResult);
					}

					if( StringUtils.isNotBlank(scope) && "SAMPLE".equals(scope) && resultList.size() >= Integer.valueOf(sampleSize) )
					{
						resultList = resultList.subList(0, Integer.valueOf(sampleSize));
						break;
					}
				}
			}
		}
		catch ( GeneralSecurityException | IOException | URISyntaxException | SAXException | TikaException e )
		{
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally
		{
			deleteFiles(downloadFilePath);
		}
		return resultList;
	}

	private String getFileContent(Drive service, FileMetaInfo fileMetaInfo, String downloadFilePath)
	{
		String fileText = "";
		String filePath = null;
		try
		{
			filePath = downloadFile(service, fileMetaInfo, downloadFilePath);
		}
		catch ( GeneralSecurityException | URISyntaxException | IOException e )
		{
			e.printStackTrace();
		}

		File docFile = new File(filePath);

		if( docFile.exists() && docFile.length() > 0 )
		{
			try
			{
				fileText = DocExtractionUtil.extractFileText(docFile);
			}
			catch ( IOException | SAXException | TikaException e )
			{
				e.printStackTrace();
			}
		}

		return fileText;
	}

	private String downloadFile(Drive service, FileMetaInfo fileMetaInfo, String downloadFilePath) throws GeneralSecurityException, URISyntaxException, IOException
	{
		String filePath = "";
		OutputStream os = null;
		try
		{
			filePath = downloadFilePath + "/" + fileMetaInfo.getFileName();
			os = new BufferedOutputStream(new FileOutputStream(filePath));
			service.files().get(fileMetaInfo.getFileId()).executeMediaAndDownloadTo(os);

		}
		catch ( IOException e )
		{
			e.printStackTrace();
			throw e;
		}
		finally
		{
			if( null != os )
			{
				os.flush();
				os.close();
			}
		}

		return filePath;
	}

	private Credential getConnection(final String userName, final String serviceAccountName, final String serviceAccountPrivateKey) throws IOException, GeneralSecurityException, URISyntaxException
	{

		Set<String> scopes = new HashSet<String>();
		scopes.add(DriveScopes.DRIVE);
		scopes.add(DriveScopes.DRIVE_METADATA);
		scopes.add(DriveScopes.DRIVE_FILE);

		GoogleCredential credential = new GoogleCredential.Builder().setTransport(GoogleNetHttpTransport.newTrustedTransport()).setJsonFactory(new JacksonFactory()).setServiceAccountId(serviceAccountName).setServiceAccountPrivateKey(getPrivateKey(serviceAccountPrivateKey))
				.setServiceAccountScopes(scopes).setServiceAccountUser(userName).build();

		return credential;
	}

	private PrivateKey getPrivateKey(String serviceAccountPrivateKey) throws NoSuchAlgorithmException, InvalidKeySpecException
	{

		serviceAccountPrivateKey = serviceAccountPrivateKey.replaceAll("\\n", "").replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "");
		KeyFactory kf = KeyFactory.getInstance("RSA");

		PKCS8EncodedKeySpec keySpecPKCS8 = new PKCS8EncodedKeySpec(Base64.decodeBase64(serviceAccountPrivateKey));
		PrivateKey privKey = kf.generatePrivate(keySpecPKCS8);

		return privKey;
	}

	private Map<String, String> getFoldersMetaInfo(Drive service) throws Exception
	{

		Map<String, String> folderMetaInfo = new HashMap<>();
		try
		{
			com.google.api.services.drive.Drive.Files.List request = service.files().list();
			FileList result;
			do
			{
				request.setQ("mimeType = 'application/vnd.google-apps.folder'");
				request.setPageSize(1000);
				request.setFields("files(id, name), nextPageToken");
				result = request.execute();
				if( result != null )
				{
					request.setPageToken(result.getNextPageToken());

					List<com.google.api.services.drive.model.File> tempFilesList = result.getFiles();
					for (com.google.api.services.drive.model.File file : tempFilesList)
					{
						folderMetaInfo.put(file.getId(), file.getName());
					}
				}
			}
			while (request.getPageToken() != null && request.getPageToken().length() > 0);
		}
		catch ( Exception e )
		{
			throw new Exception(e.getMessage());
		}

		return folderMetaInfo;
	}

	private String getFilePath(final Drive service, String parentFileId, Map<String, String> foldersMetainfo) throws Exception
	{

		List<String> filePathList = new ArrayList<>();

		String tempFileId = parentFileId;

		try
		{

			if(null != foldersMetainfo.get(parentFileId))
			{
				filePathList.add(foldersMetainfo.get(parentFileId));
			}

			com.google.api.services.drive.Drive.Files.List request = service.files().list();
			com.google.api.services.drive.model.File result;
			request.setFields("files(id, name, parents), nextPageToken");
			boolean isRootFolder = false;
			do
			{
				result = service.files().get(tempFileId).setFields("id,name,parents").execute();
				if( result != null )
				{
					if( null == result.getParents() || result.getParents().isEmpty())
					{
						isRootFolder = true;
					} 
					else
					{
						List<String> fileParents = result.getParents();

						if(null != fileParents)
						{
							for(String path: fileParents)
							{
								if(null != foldersMetainfo.get(path))
								{
									filePathList.add(foldersMetainfo.get(path));
									tempFileId = path;
								} else 
								{
									isRootFolder = true;
								}
							}
						} else 
						{
							isRootFolder = true;
						}
					}
				}
			} while (!isRootFolder);

		}
		catch ( Exception e )
		{
			throw new Exception(e.getMessage());
		}

		filePathList.add("My Drive");
		Collections.reverse(filePathList);
		String filePath = String.join("/", filePathList);

		return filePath;
	}

}