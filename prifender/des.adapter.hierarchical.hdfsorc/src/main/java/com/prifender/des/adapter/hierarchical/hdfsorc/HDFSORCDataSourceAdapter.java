package com.prifender.des.adapter.hierarchical.hdfsorc;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
public final class HDFSORCDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "HDFSORC";
	Configuration config;
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("HDFSORC")
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
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS ORC connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(config);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS ORC.");
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
		    config.set("fs.file.impl",  org.apache.hadoop.fs.LocalFileSystem.class.getName());
		    Path path = new Path(filePath);
		    return path; 
	}

	public Metadata metadataByConnection(Path path,String fileName) throws DataExtractionServiceException, IOException {
		Metadata metadata = new Metadata();
		List<String> fileNameList = new ArrayList<>();
		Reader fileReader ;
		
		try { 
			
			if(path != null){
				fileNameList.add(fileName);
			}
			
			ReaderOptions opts = OrcFile.readerOptions(config);
			fileReader = OrcFile.createReader(path, opts);
					
			StructObjectInspector inspector = (StructObjectInspector)fileReader.getObjectInspector();
			
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
			for (NamedType namedType : fNameHeaderList) {
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getColumnsForFile(fileReader,inspector);
				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
			}
			metadata.setObjects(namedTypeObjectsList);
	    
		} catch (IOException  e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}
		return metadata;
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
				DataExtractionThread dataExtractionExecutor = new HDFSORCDataExtractionExecutor(ds, spec, job, dataSize,adapterHome,messaging,encryptionServiceClient);
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
	
	@Override
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		Path path = null;
		int countRows = 0;
		
		try {
			path = getDataBaseConnection(ds);
			if (path != null) {
				ReaderOptions opts = OrcFile.readerOptions(config);
				Reader fileReader = OrcFile.createReader(path, opts);
				countRows = (int) getRowCount(fileReader,tableName);
				
			}
		} catch (ClassNotFoundException | SQLException | IOException e ) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		}  finally {
			destroy(config);
		}

		return countRows;
	}
	
	private List<NamedType> getColumnsForFile(Reader fileReader, StructObjectInspector inspector) {
		List<NamedType> namedTypeList = new ArrayList<NamedType>();
		
		StructObjectInspector insp = (StructObjectInspector) fileReader.getObjectInspector();
	
		List<? extends StructField> structFieldList = insp.getAllStructFieldRefs();
		
		for(StructField structField : structFieldList){
			
			NamedType namedType = new NamedType();
			String structFieldColName = structField.getFieldName();
			ObjectInspector objectInspector = structField.getFieldObjectInspector();
			
			String type = objectInspector.getTypeName().toUpperCase();
			Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(type));
			
			String category = objectInspector.getCategory().toString();
			
			if(StringUtils.equals(category, ObjectInspector.Category.LIST.toString())){
				
				namedType.setName(structFieldColName);
				namedType.setType(new Type().kind(Type.KindEnum.LIST));
				Type entryType = new Type().kind(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				
				List<NamedType> attributeListForColumns = getSubObjectFieldList(objectInspector);
				entryType.setAttributes(attributeListForColumns);
				namedTypeList.add(namedType);
			
			}else{
				namedType.setName(structFieldColName);
				namedType.setType(columnType);
				namedTypeList.add(namedType);
			}
		}

		return namedTypeList;
	}

	private List<NamedType> getSubObjectFieldList(ObjectInspector objectInspector) {
		List<NamedType> namedTypeList = new ArrayList<NamedType>();

		ListObjectInspector subListObjectInspector = (ListObjectInspector) objectInspector;
		String category = subListObjectInspector.getListElementObjectInspector().getTypeName();
		if (StringUtils.equals(category.toUpperCase(), "STRING")) {
			
			NamedType namedType = new NamedType();
			Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType("STRING"));
			namedType.setType(columnType);

			namedTypeList.add(namedType);
		} else {

			StructObjectInspector subStructObjectInspector = (StructObjectInspector) subListObjectInspector
					.getListElementObjectInspector();
			List<? extends StructField> subStructFieldList = subStructObjectInspector.getAllStructFieldRefs();
			for (StructField subStructField : subStructFieldList) {
				NamedType namedType = new NamedType();

				ObjectInspector subObjectInspector = subStructField.getFieldObjectInspector();
				String type = subObjectInspector.getTypeName().toUpperCase();
				Type columnType = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(type));

				namedType.setName(subStructField.getFieldName());
				namedType.setType(columnType);
				namedTypeList.add(namedType);
			}
		}
		return namedTypeList;
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
	
	

	private long getRowCount(Reader rdr, String fName){
		long tempCount = 0;
		tempCount = rdr.getNumberOfRows();
		return tempCount;
	}

}