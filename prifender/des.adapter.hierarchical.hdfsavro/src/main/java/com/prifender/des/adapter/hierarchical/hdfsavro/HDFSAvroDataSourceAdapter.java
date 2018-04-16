package com.prifender.des.adapter.hierarchical.hdfsavro;
import java.io.IOException;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataExtractionUtil;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;
import com.prifender.des.util.DatabaseUtil;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component
public final class HDFSAvroDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "HDFSAvro";
	Configuration config;
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("HDFSAvro")
			.addConnectionParamsItem(new ConnectionParamDef().id("Host").label("Host").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Port").label("Port").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("UserName").label("UserName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("DfsReplication").label("DfsReplication").type(TypeEnum.INTEGER))
	        .addConnectionParamsItem(new ConnectionParamDef().id("FilePath").label("FilePath").type(TypeEnum.STRING));

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public ConnectionStatus testConnection(final DataSource ds) throws DataExtractionServiceException {
		Path path = null;
		try {
			path = getDataBaseConnection(ds);
			if (path != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS Avro connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(config);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS Avro.");
	}
	
	public void destroy(Configuration config){
		if(config != null){
			config.clear();
		}
	}

	@Override
	public Metadata getMetadata(final DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		Path path = null;
		try {
				path = getDataBaseConnection(ds);
				final String filePath = databaseUtilService.getConnectionParam(ds, "FilePath");
				metadata = metadataByConnection(path,filePath);
			if (metadata == null) {
				throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(config);
		}
		return metadata;
	}

	public Path getDataBaseConnection(DataSource ds)
			throws SQLException, ClassNotFoundException, DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String hostName = databaseUtilService.getConnectionParam(ds, "Host");
		final String portNumber = databaseUtilService.getConnectionParam(ds, "Port");
		final String dfsReplication = databaseUtilService.getConnectionParam(ds, "DfsReplication");
		final String filePath = databaseUtilService.getConnectionParam(ds, "FilePath");
		return getDataBaseConnection(hostName, portNumber, dfsReplication, filePath);
	}

	public Path getDataBaseConnection(String hostName, String port,String dfsReplication, String filePath) throws SQLException, ClassNotFoundException {
		    config = new Configuration();
		    config.set("fs.defaultFS", "hdfs://"+hostName);
		    config.setInt("dfs.replication",Integer.valueOf(dfsReplication));
		    config.set("fs.hdfs.impl",DistributedFileSystem.class.getName());
		    config.set("fs.file.impl",  org.apache.hadoop.fs.LocalFileSystem.class.getName()  );
		    Path path = new Path(filePath);
		    return path; 
	}

	public Metadata metadataByConnection(Path path,String fileName) throws DataExtractionServiceException, IOException {
		Metadata metadata = new Metadata();
		List<String> fileNameList = new ArrayList<>();
		FileReader<GenericRecord> fileReader = null;
		try { 
			
			if(path != null){
				fileNameList.add(fileName);
			}
			SeekableInput input = new FsInput(path, config);
		    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		    fileReader = DataFileReader.openReader(input, reader);
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
			for (NamedType namedType : fNameHeaderList) {
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getColumnsFromFile(fileReader);
				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
	    
		} catch (IOException  e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}finally{
			fileReader.close(); 
		}
		return metadata;
	}
	private List<NamedType> getFileNameFromList(String fName) {
		List<NamedType> tableList = new ArrayList<>();
		NamedType namedType = new NamedType();
		namedType.setName(fName);
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		tableList.add(namedType);
		return tableList;
	}
	private List<NamedType> getColumnsFromFile(FileReader<GenericRecord> fileReader) throws DataExtractionServiceException {
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
		    Schema scehma = fileReader.getSchema();
		    JSONParser parser = new JSONParser(); 
		    JSONObject json = (JSONObject) parser.parse(scehma.toString());
		    JSONArray jsonArray = (JSONArray) json.get("fields");

			for(int i = 0; i < jsonArray.size(); i++)
			{
			      JSONObject object = (JSONObject) jsonArray.get(i);
			      String columnName = (String) object.get("name");
			      NamedType namedType = new NamedType();
				  namedType.setName(columnName);
			      JSONArray typeArray = (JSONArray) object.get("type");
			      if(typeArray.get(1) instanceof JSONObject){
			    	  namedType.setType(new Type().kind(Type.KindEnum.LIST));
					  List<NamedType> attributeListForColumns = new ArrayList<>();
			    	  JSONObject subObject = (JSONObject) typeArray.get(1);
			    	  JSONArray subArray = (JSONArray) subObject.get("items");
			    	  if(subArray.get(1) instanceof String){
			    		    String type=  (String)subArray.get(1);
			    		    Type childCloumnType = new Type().kind(Type.KindEnum.LIST).dataType(databaseUtilService.getDataType(type.toUpperCase()));
							namedType.setType(childCloumnType);
			    	  }else if(subArray.get(1) instanceof JSONObject){
			    		  Type entryType = new Type().kind(Type.KindEnum.OBJECT);
						  namedType.getType().setEntryType(entryType);
			    		  JSONObject subArrayObject= (JSONObject)subArray.get(1);
			    		  JSONArray colmnsAndTypes = (JSONArray) subArrayObject.get("fields");
					  		 for(int j = 0; j < colmnsAndTypes.size(); j++)
							{
					  			JSONObject objectName = (JSONObject) colmnsAndTypes.get(j);
					  			String subColumn = (String) objectName.get("name");
					  			String docType = (String) objectName.get("doc");
					  			NamedType childNamedType = new NamedType();
								childNamedType.setName(subColumn);
								Type childCloumnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(docType.toUpperCase()));
								childNamedType.setType(childCloumnType);
								attributeListForColumns.add(childNamedType);
							} 
					  		entryType.setAttributes(attributeListForColumns);
			    	  }
			    	 
			      }else{
			    	  Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(typeArray.get(1).toString().toUpperCase()));
			    	  namedType.setType(typeForCoumn);
			      }
			  	attributeList.add(namedType);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}

		return attributeList;
	}
	 
	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {

		int countRows = 0;
		Path path = null;
		FileReader<GenericRecord> fileReader = null;
		try {
			path = getDataBaseConnection(ds);
		    SeekableInput input = new FsInput(path, config);
		    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		    fileReader = DataFileReader.openReader(input, reader);
		    for (GenericRecord datum : fileReader) {
		    	if(datum != null) 
		    	 countRows++;
		      }
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} catch (IOException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			try {
				if(fileReader != null)
				fileReader.close();
				destroy(config);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return countRows;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging)
			throws DataExtractionServiceException {
		StartResult startResult = null;
		try {

			String tableName = spec.getCollection();
			int dataSize = getCountRows(ds, tableName);
			
			if (dataSize == 0) {
				throw new DataExtractionServiceException(new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			
			String[] schemaTableName = StringUtils.split(tableName, "." );
			tableName = schemaTableName[schemaTableName.length-1];
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new HDFSAvroDataExtractionExecutor(ds, spec, job, dataSize,adapterHome,messaging,encryptionServiceClient);
				executor.execute(dataExtractionExecutor);
				startResult = new StartResult(job, dataExtractionExecutor);
			} catch (Exception err) {
				throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
			} finally {
				executor.shutdown();
				executor.awaitTermination(1, TimeUnit.SECONDS);
			}
		} catch (InterruptedException err) {
			throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
		}
		return startResult;
	}

}