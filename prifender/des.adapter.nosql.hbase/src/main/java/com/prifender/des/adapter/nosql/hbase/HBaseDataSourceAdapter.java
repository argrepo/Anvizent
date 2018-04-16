package com.prifender.des.adapter.nosql.hbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.controller.DataExtractionUtil;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParam;
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
import com.prifender.des.model.Type.DataTypeEnum;
import com.prifender.des.model.Type.KindEnum;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnectionFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
@Component
public final class HBaseDataSourceAdapter implements DataSourceAdapter {
	@Value( "${des.home}" )
	private String desHome;
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final String TYPE_ID = "HBase";
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("HBase")
			.addConnectionParamsItem(new ConnectionParamDef().id("HBaseZookeeperQuorum").label("HBaseZookeeperQuorum").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("HBaseZookeeperPropertyClientPort").label("HBaseZookeeperPropertyClientPort").type(TypeEnum.STRING));

	@SuppressWarnings("resource")
	@Override
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		int countRows = 0;
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) {
				AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
		        Scan scan = new Scan();
		        final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =   new LongColumnInterpreter();
			    countRows = (int) aggregationClient.rowCount(new HTable(connection.getConfiguration(), tableName), ci, scan);
			}
		} catch ( Throwable e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(connection);
		}
		return countRows;
	}
	 
	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}

	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;
		Metadata metadata = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) { 
				metadata = metadataByConnection(connection.getAdmin(),connection.getConfiguration());
				if (metadata == null) {
					throw new DataExtractionServiceException(new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (IOException e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error").message(e.getMessage()));
		} finally {
			destroy(connection);
		}
		return metadata;
	}

	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging) throws DataExtractionServiceException {

		StartResult startResult = null;
		try {
			
			String tableName = spec.getCollection();
			int dataSize = getCountRows(ds, tableName);
			if (dataSize == 0) {
				throw new DataExtractionServiceException(new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				final String adapterHome = DataExtractionUtil.createDir(this.desHome , TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new HBaseDataExtractionExecutor(ds, spec, job, dataSize,adapterHome,messaging,encryptionServiceClient);
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
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		Connection connection = null;
		try {
			connection = getDataBaseConnection(ds);
			if (connection != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("Hbase connection successfully established.");
			}
		} catch (IOException e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(connection);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to Hbase database");
	} 

	public void destroy(Connection conn) {
		if (conn != null) {
			try {
				conn.getAdmin().close();
				conn.getConfiguration().clear();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private  Connection getDataBaseConnection(DataSource ds) throws DataExtractionServiceException, IOException {
		Connection con = null;
		try{
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		final String hBaseZookeeperQuorum = getConnectionParam(ds, "HBaseZookeeperQuorum");
		final String hBaseZookeeperPropertyClientPort = getConnectionParam(ds, "HBaseZookeeperPropertyClientPort");
	 
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", hBaseZookeeperQuorum);
		config.set("hbase.zookeeper.property.clientPort", hBaseZookeeperPropertyClientPort);
		HBaseAdmin.checkHBaseAvailable(config);
		con = ConnectionFactory.createConnection(config);
		}catch(ConnectException | UnknownHostException  une){
			throw new DataExtractionServiceException(new Problem().code("connecion error.").message(une.getMessage()));
		}catch(Exception e){
			throw new DataExtractionServiceException(new Problem().code("connecion error.").message(e.getMessage()));
		}
		return  con;
	  }
	protected static final String getConnectionParam(final DataSource ds, final String param) {
		if (ds == null) {
			throw new IllegalArgumentException();
		}

		if (param == null) {
			throw new IllegalArgumentException();
		}

		for (final ConnectionParam cp : ds.getConnectionParams()) {
			if (cp.getId().equals(param)) {
				return cp.getValue();
			}
		}

		return null;
	}
	
	private Metadata metadataByConnection(Admin hBaseAdmin,Configuration config) throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		try {
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
		    List<NamedType> tableList = getDatasourceRelatedTables(hBaseAdmin);

			for (NamedType namedType : tableList) {
				Type entryType = entryType(Type.KindEnum.OBJECT);
				namedType.getType().setEntryType(entryType);
				List<NamedType> attributeListForColumns = getTableRelatedColumns(namedType.getName(), config);

				entryType.setAttributes(attributeListForColumns);
				namedTypeObjectsList.add(namedType);
				metadata.setObjects(namedTypeObjectsList);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}
		return metadata;
	}
	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}
	@SuppressWarnings("deprecation")
	private List<NamedType> getTableRelatedColumns(String tableName,Configuration config) throws DataExtractionServiceException, IOException {
		List<NamedType> attributeList = new ArrayList<NamedType>();
		HTable hTable = null;
		Set<String> columns = new HashSet<String>();
		try {
			 hTable=new HTable(config, tableName);
			 
			 ResultScanner results = hTable.getScanner(new Scan().setFilter(new PageFilter(1000)));
			 for(Result result:results){
	             for(KeyValue keyValue:result.list()){
	            	 columns.add(Bytes.toString(keyValue.getFamily())  + ":" +  Bytes.toString(keyValue.getQualifier()));
	            } 
	        }
			 for(String column : columns){
					NamedType attributeForColumn = new NamedType();
    				attributeForColumn.setName(column);
    				Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(column.getClass().getSimpleName()));
    				attributeForColumn.setType(typeForCoumn);
    				attributeList.add(attributeForColumn);
			 }
			 
		}catch(Exception e){
			throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}finally{
			if(hTable != null){
				hTable.close();
			}
		}
		return attributeList;
	
	}
	private List<NamedType> getDatasourceRelatedTables(Admin hBaseAdmin) 	throws DataExtractionServiceException {
		List<NamedType> tableList = new ArrayList<>();
		try {
			HTableDescriptor[] tDescriptor = hBaseAdmin.listTables();
			for (int k=0; k<tDescriptor.length;k++ ){
				NamedType namedType = new NamedType();
				namedType.setName(tDescriptor[k].getNameAsString());
				Type type = new Type().kind(Type.KindEnum.LIST);
				namedType.setType(type);
				tableList.add(namedType);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("Datasource Error").message(e.getMessage()));
		}
		return tableList;
	}
	private DataTypeEnum getDataType(String dataType) {
		DataTypeEnum dataTypeEnum = null;
		if (dataType.equals("INT") || dataType.equals("number") || dataType.equals("Number")
				|| dataType.equals("Integer") || dataType.equals("INTEGER")) {
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		} else if (dataType.equals("Decimal")) {
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		} else if (dataType.equals("Float") || dataType.equals("Double") || dataType.equals("Numeric")
				|| dataType.equals("Long") || dataType.equals("Real")) {
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		} else if (dataType.equals("String")) {
			dataTypeEnum = Type.DataTypeEnum.STRING;
		} else if (dataType.equals("Boolean")) {
			dataTypeEnum = Type.DataTypeEnum.BOOLEAN;
		}
		return dataTypeEnum;
	}
}
