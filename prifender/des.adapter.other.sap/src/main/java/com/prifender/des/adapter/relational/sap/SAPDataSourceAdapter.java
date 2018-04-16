package com.prifender.des.adapter.relational.sap;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFieldIterator;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoRecordFieldIterator;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataProvider;

@Component
public final class SAPDataSourceAdapter implements DataSourceAdapter {
    
    @Value( "${des.home}" )
    private String desHome;
    @Autowired
	EncryptionServiceClient encryptionServiceClient;
    
    private static final String TYPE_ID = "SAP";
    static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
     
    private static final DataSourceType TYPE = new DataSourceType()
	        .id( TYPE_ID )
	        .label( "SAP" )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "PoolName" ).label( "PoolName" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "MaxConnections" ).label( "MaxConnections" ).type( TypeEnum.STRING) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Client" ).label( "Client" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "UserName" ).label( "UserName" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Password" ).label( "Password" ).type( TypeEnum.PASSWORD ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Host" ).label( "Host" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Port" ).label( "Port" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "SystemNumber" ).label( "SystemNumber" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Language" ).label( "Language" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "Repository" ).label( "Repository" ).type( TypeEnum.STRING ) )
	        .addConnectionParamsItem( new ConnectionParamDef().id( "FunctionName" ).label( "FunctionName" ).type( TypeEnum.STRING ));
 	
    @Override
		public DataSourceType getDataSourceType() {
			return TYPE;
		}
	   
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		JCoDestination destination = null;
		if (ds == null) {
			throw new DataExtractionServiceException(new Problem().code("datasource error").message("datasource is null"));
		}
		try {
			  destination = getDatabaseConnection(ds);
			  final String functionName = getConnectionParam(ds, "FunctionName");
			if (destination.getRepository().getFunction(functionName) != null) {
				return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS).message("SAP connection successfully established.");
			}
		} catch (Exception e) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message(e.getMessage());
		} finally {
			destroy(destination);
		}
		return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE).message("Could not connect to SAP database");
	}
	
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		JCoDestination destination = null;

		Metadata metadata = new Metadata();
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}

		try {
			destination = getDatabaseConnection(ds);
			if (destination != null) {
				metadata = metadataByConnection(ds,destination);
				if (metadata == null) {
					throw new DataExtractionServiceException(
							new Problem().code("metadata error").message("meta data not found for connection."));
				}
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		}  finally {
			destroy(destination);
		} 

		return metadata;
	}
		
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging)
			throws DataExtractionServiceException {
		StartResult startResult = null;
		try {
			final String tableName = spec.getCollection();
			int dataSize =  getCountRows(ds, tableName) ;
			if (dataSize != 0) {
				DataExtractionJob job = new DataExtractionJob()
						.id(spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString())
						.state(DataExtractionJob.StateEnum.WAITING);
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					final String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
					DataExtractionThread dataExtractionExecutor = new SAPDataExtractionExecutor(ds, spec, job,
							dataSize, adapterHome,messaging,encryptionServiceClient);
					executor.execute(dataExtractionExecutor);
					startResult = new StartResult(job, dataExtractionExecutor);
				} catch (Exception err) {
					throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
				}
				executor.shutdown();
				executor.awaitTermination(1, TimeUnit.SECONDS);
			} else {
				throw new DataExtractionServiceException(new Problem().code("meta data error").message("No Rows Found in table :" + tableName));
			}
		} catch (InterruptedException err) {
			throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
		}
		return startResult;
	}

	private JCoDestination getDatabaseConnection(DataSource ds) {

		final String hostName = getConnectionParam(ds, "Host");
		final String systemNumber = getConnectionParam(ds, "SystemNumber");
		final String userName = getConnectionParam(ds, "UserName");
		final String password = getConnectionParam(ds, "Password");
		final String client = getConnectionParam(ds, "Client");
		final String language = getConnectionParam(ds, "Language");

		JCoDestination destination = null;
		Properties connectProperties = new Properties();
		connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, hostName);
		connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, systemNumber);
		connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, client);
		connectProperties.setProperty(DestinationDataProvider.JCO_USER, userName);
		connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, password);
		connectProperties.setProperty(DestinationDataProvider.JCO_LANG, language);
		connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, "8");
	    connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT,"8000");
	    createDataFile(ABAP_AS, "jcoDestination", connectProperties);
	    connectProperties.clear();
		try {
			destination = JCoDestinationManager.getDestination(ABAP_AS);
		    JCoContext.begin(destination);
		} catch (JCoException e) {
			e.getMessage();
		}

		return destination;
	}

	private Metadata metadataByConnection(DataSource ds,JCoDestination destination) throws DataExtractionServiceException {
		
		final String functionName = getConnectionParam(ds, "FunctionName");
		
		Metadata metadata = new Metadata();
		List<String> functionList = null;
		try {
			
			if(functionName != null){
				functionList = new ArrayList<String>();
				functionList.add(functionName);
				}else{
					throw new DataExtractionServiceException( new Problem().code("datasource not found").message("datasource not found in connection params.") );
				}
			
			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			
			for(String function : functionList){
				
			  JCoFunction jcoFunction = destination.getRepository().getFunction(function);
			  List<JCoTable> jcoTableList = getJcoFunctionRelatedTables(jcoFunction);
			  for(JCoTable tableInfo : jcoTableList){
					//table entry type
				    NamedType namedType = new  NamedType();
				    
				    Type entryType = entryType(Type.KindEnum.OBJECT);
				    namedType.setName(tableInfo.getRecordMetaData().getName());
					Type type = new Type().kind(Type.KindEnum.LIST);
					namedType.setType(type);
					
					List<NamedType> attributeListForColumns = getJcoTableRelatedColumns(tableInfo);
					
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
					namedType.getType().setEntryType(entryType);
					metadata.setObjects(namedTypeObjectsList);
				}
			  }
		  } catch (Exception e) {
			 throw new DataExtractionServiceException(new Problem().code("metadata error.").message(e.getMessage()));
		}
		return metadata;
	}

	private List<JCoTable> getJcoFunctionRelatedTables(JCoFunction jcoFunction) {
		JCoFieldIterator codes = jcoFunction.getTableParameterList().getFieldIterator();
		List<JCoTable> jcoTableList = new ArrayList<JCoTable>();
		while (codes.hasNextField()) {
			JCoTable tbl = codes.nextField().getTable();
			jcoTableList.add(tbl);
		}
		return jcoTableList;
	}
	
	public void destroy(JCoDestination destination){
		if (destination != null) {
			try {
				// destination.getRepository().clear();
				JCoContext.end(destination);
			} catch (JCoException e) {
				e.printStackTrace();
			}
		}
	}
	
	private List<NamedType> getJcoTableRelatedColumns(JCoTable tableInfo) throws DataExtractionServiceException {

		List<NamedType> attributeList = new ArrayList<NamedType>();
		try {
			if (tableInfo.getRecordMetaData().getFieldCount() > 0) {
				do {
					for (JCoRecordFieldIterator rcdFld = tableInfo.getRecordFieldIterator(); rcdFld.hasNextField();) {
						NamedType attributeForColumn = new NamedType();
						JCoField tabField = rcdFld.nextField();
						attributeForColumn.setName(tabField.getName());
						Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(tabField.getTypeAsString()));
						attributeForColumn.setType(typeForCoumn);
						attributeList.add(attributeForColumn);
					}
				} while (tableInfo.nextRow() == true);
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(
					new Problem().code("columns getting error").message(e.getMessage()));
		}
		return attributeList;
	}
	
	
	private Type entryType(KindEnum kindEnum) {
		return new Type().kind(kindEnum);
	}
	
	private DataTypeEnum getDataType(String dataType){
		DataTypeEnum dataTypeEnum = null;
		if(dataType.equals("INT") || dataType.equals("INT1") || dataType.equals("INT2")){
			dataTypeEnum = Type.DataTypeEnum.INTEGER;
		}else if(dataType.equals("BCD")){
			dataTypeEnum = Type.DataTypeEnum.DECIMAL;
		}else if(dataType.equals("FLOAT")){
			dataTypeEnum = Type.DataTypeEnum.FLOAT;
		}else if(dataType.equals("CHAR") || dataType.equals("NUM") || dataType.equals("STRING") || dataType.equals("BYTE") ||  dataType.equals("XSTRING")){
			dataTypeEnum = Type.DataTypeEnum.STRING;
		} else if(dataType.equals("DATE")){
			  dataTypeEnum = Type.DataTypeEnum.DATE;
		}else if(dataType.equals("TIME")){
			  dataTypeEnum = Type.DataTypeEnum.TIME;
		}
		return dataTypeEnum;
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

	@Override
	public int getCountRows(DataSource ds, String tableName)
			throws DataExtractionServiceException {
		int countRows = 0;
		JCoDestination destination = null;
		try {
			destination = getDatabaseConnection(ds);
			if (destination != null) {

				final String functionName = getConnectionParam(ds, "FunctionName");
				JCoFunction jcoFunction = destination.getRepository().getFunction(functionName);
			    if (jcoFunction != null) {
					jcoFunction.execute(destination);
				} 	
				JCoFieldIterator jCoFieldIterator = jcoFunction.getTableParameterList().getFieldIterator();
				boolean isTableFound = false;
				while (jCoFieldIterator.hasNextField()) {
					    JCoField jCoField = jCoFieldIterator.nextField();
					    JCoTable jcoTable = jCoField.getTable();
						if(jcoTable.getRecordMetaData().getName().equals(tableName)){
					        	  JCoTable tableInfo = (JCoTable)jCoField.getValue();
					        	  countRows = tableInfo.getNumRows();
					        	  isTableFound = true;
					        	  break;
						}
				}
				if (countRows <= 0 ) {
					throw new DataExtractionServiceException(new Problem().code("No Records Found").message(" Table name "+tableName+" is not found for the given Function "+functionName));
				}
	            if(!isTableFound){
					throw new DataExtractionServiceException(new Problem().code("TableName not Found").message(" Table name "+tableName+" is not found for the given Function "+functionName));
				} 
			}
		} catch (Exception e) {
			throw new DataExtractionServiceException(new Problem().code("connection error").message(e.getMessage()));
		} finally {
			destroy(destination);
		} 
		return countRows;
	} 
	 static void createDataFile(String name, String suffix, Properties properties)
	    {
	        File cfg = new File(name+"."+suffix );
	            try
	            {
	                FileOutputStream fos = new FileOutputStream(cfg, false);
	                properties.store(fos, "for tests only !");
	                fos.close();
	            }
	            catch (Exception e)
	            {
	                throw new RuntimeException("Unable to create the destination file " + cfg.getName(), e);
	            }
	    }

}
