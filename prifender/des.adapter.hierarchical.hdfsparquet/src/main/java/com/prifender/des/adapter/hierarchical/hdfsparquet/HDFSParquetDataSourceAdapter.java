package com.prifender.des.adapter.hierarchical.hdfsparquet;
import java.io.IOException;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
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
public final class HDFSParquetDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "HDFSParquet";
	Configuration config;
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("HDFSParquet")
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
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("HDFS Parquet connection successfully established.");
			}
		} catch (ClassNotFoundException | SQLException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(config);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to HDFS Avro.");
	}
	
	public void destroy(Configuration config){
		config.clear();
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
		try { 
			if(path == null){
				throw new DataExtractionServiceException(
						new Problem().code("path not founnd").message("File not found in the given path."));
			}
			  ParquetMetadata readFooter = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
	          MessageType schema = readFooter.getFileMetaData().getSchema();
			  List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
				List<NamedType> fNameHeaderList = getFileNameFromList(fileName);
				for (NamedType namedType : fNameHeaderList) {
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getColumnsFromFile(schema);
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);
		    
		} catch (IOException  e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
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
	
	private List<NamedType> getColumnsFromFile(MessageType schema) throws DataExtractionServiceException {
		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
		     
		    List<org.apache.parquet.schema.Type> list = schema.getFields();
             Map<String,String> map = new HashMap<String,String>();
             List<ColumnDescriptor> columnDescriptorList = schema.getColumns();
             for(ColumnDescriptor columnDescriptor : columnDescriptorList){
            	 String s = columnDescriptor.toString();
            	  for(org.apache.parquet.schema.Type type : list){
                     if(s.contains(type.getName())){
                    	 if(s.contains("bag")){
                    		 String result = columnDescriptor.toString().substring(0, columnDescriptor.toString().indexOf("]"));
                    		 String[] names = result .split(",");
                    		 String name  = names[names.length-1];
                    		 map.put(type.getName().toString()+"."+name.trim(), columnDescriptor.getType().toString());
                    	 }else{
                    		 map.put(type.getName().toString(), columnDescriptor.getType().toString());
                    	 }
                      }
                    }  
                 } 
             for (Map.Entry<String,String> entry : map.entrySet()) {
                NamedType attributeForColumn = new NamedType();
				attributeForColumn.setName(entry.getKey());
				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(entry.getValue().toUpperCase()));
				attributeForColumn.setType(typeForCoumn);
				attributeList.add(attributeForColumn);
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
		try {
			path = getDataBaseConnection(ds);
			ParquetMetadata readFooter = ParquetFileReader.readFooter(config, path, ParquetMetadataConverter.NO_FILTER);
            ParquetFileReader parquetFileReader = new ParquetFileReader(config, path, readFooter);
            PageReadStore pages = null;
                while (null != (pages = parquetFileReader.readNextRowGroup())) {
                     countRows = (int) pages.getRowCount();
                }
             
		} catch (ClassNotFoundException | SQLException e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} catch (IOException e1) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e1.getMessage()));
		} finally {
			destroy(config);
		}
       return countRows;
	}
	 


	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging)
			throws DataExtractionServiceException {
		StartResult startResult = null;
		try {

			String tableName = spec.getCollection();
			int totalRecords = getCountRows(ds, tableName);
			
			if (totalRecords == 0) {
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
				DataExtractionThread dataExtractionExecutor = new HDFSParquetDataExtractionExecutor(ds, spec, job, totalRecords,adapterHome,messaging,encryptionServiceClient);
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