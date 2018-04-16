package com.prifender.des.adapter.relational.sap;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Problem;
import com.prifender.des.util.CommonUtil;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

public class SAPDataExtractionExecutor extends DataExtractionThread {

	private static SAPDataExtractionETLJobExecutor etlJobUtil = null;
	
	private final static String JOB_NAME = "local_project.prifender_sap_v1_0_1.Prifender_SAP_v1";
	private final static String DEPENDENCY_JAR = "prifender_sap_v1_0_1.jar";
	
	DataSource ds;
	int dataSize;
	int chunkSize;
	String adapterHome;
	DataExtractionSpec.ScopeEnum scope;
	MessagingConnectionFactory messaging;
	EncryptionServiceClient encryptionServiceClient;
	public SAPDataExtractionExecutor(DataSource ds, final DataExtractionSpec spec, final DataExtractionJob job,
			final int dataSize, final String adapterHome,MessagingConnectionFactory messaging,EncryptionServiceClient encryptionServiceClient) throws DataExtractionServiceException {
		super(spec, job, dataSize);
		etlJobUtil = new SAPDataExtractionETLJobExecutor(adapterHome);
		this.ds = ds;
		this.dataSize = dataSize;
		this.adapterHome = adapterHome;
		if (spec.getScope() == null) {
			spec.setScope(DataExtractionSpec.ScopeEnum.ALL);
		}
		this.scope = spec.getScope();
		this.chunkSize = this.dataSize;
		this.messaging=messaging;
		this.encryptionServiceClient=encryptionServiceClient;
		if (spec.getScope().equals(DataExtractionSpec.ScopeEnum.SAMPLE)) {
			if (spec.getSampleSize() == null) {
				throw new DataExtractionServiceException(
						new Problem().code("Meta data error").message("sampleSize value not found"));
			}
			this.chunkSize = this.dataSize < spec.getSampleSize() ? this.dataSize : spec.getSampleSize();
		}
	}

	@Override
	public void run() {
		
		final String queueName = "DES-" + this.job.getId();
		
		synchronized (this.job) {
			this.job.setTimeStarted(DATE_FORMAT.format(new Date()));
			this.job.setOutputMessagingQueue(queueName);
			this.job.setObjectCount(this.chunkSize);
			this.job.setObjectsExtracted(0);
		}
		try( final MessagingConnection messagingServiceConnection = this.messaging.connect() ){
			 final String scheduleQueueName = CommonUtil.getScheduleQueueName();
			 final MessagingQueue queue = messagingServiceConnection.queue( scheduleQueueName );
			final String jobFilesPath = adapterHome;
			StringBuffer stringBuffer = new StringBuffer();
			for (final DataExtractionAttribute attribute : spec.getAttributes()) {
				List<DataExtractionAttribute> childAttributeList = attribute.getChildren();
				if (childAttributeList != null && childAttributeList.size() > 0) {
					for (DataExtractionAttribute childAttribute : childAttributeList) {
						stringBuffer.append(attribute.getName() + "." + childAttribute.getName()).append(",");
					}
				} else {
					stringBuffer.append(attribute.getName()).append(",");
				}
			}
			String dataSourceColumnNames = removeLastChar(stringBuffer.toString());
			final String dataSourceHost = getConnectionParam(ds, "Host");
			final String dataSourcePort = getConnectionParam(ds, "Port");
			final String dataSourceUser = encryptionServiceClient.encrypt(getConnectionParam(ds, "UserName"));
			final String dataSourcePassword = encryptionServiceClient.encrypt(getConnectionParam(ds, "Password"));
			final String client = getConnectionParam(ds, "Client");
			final String language = getConnectionParam(ds, "Language");
			final String systsemNumber = getConnectionParam(ds, "SystemNumber");

			 int chunkCount = 0;
			 if(this.scope.equals(DataExtractionSpec.ScopeEnum.ALL)){
				   int sampleSizeForChunk = CommonUtil.getSampleSize(this.chunkSize);
				   int calculatedChunkSize = (this.chunkSize / sampleSizeForChunk ); 
					if(calculatedChunkSize > 1){
						for(int i=0;i<calculatedChunkSize;i++){
							int offset = Integer.valueOf(sampleSizeForChunk*i);
							Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, JOB_NAME, dataSourceUser,
									dataSourcePassword, spec.getCollection(), dataSourceColumnNames, dataSourceHost, dataSourcePort,
									dataSourceUser, job.getId(), String.valueOf(offset), String.valueOf(sampleSizeForChunk), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
									String.valueOf(sampleSizeForChunk),client,language,systsemNumber);
						chunkCount++;
						CommonUtil.postMessage(queue,contextParams,JOB_NAME,this.job.getId(),DEPENDENCY_JAR,scheduleQueueName,ds.getType(), 
								calculatedChunkSize,chunkCount);
					}
				}else{
					int offset=0;
					Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, JOB_NAME, dataSourceUser,
							dataSourcePassword, spec.getCollection(), dataSourceColumnNames, dataSourceHost, dataSourcePort,
							dataSourceUser, job.getId(), String.valueOf(offset), String.valueOf(this.chunkSize), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
							String.valueOf(this.chunkSize),client,language,systsemNumber);
					chunkCount++;
					CommonUtil.postMessage(queue,contextParams,JOB_NAME,this.job.getId(),DEPENDENCY_JAR,scheduleQueueName,ds.getType(),
							1,chunkCount);
				
				}
			 }else{
				 int offset=0;
				 Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, JOB_NAME, dataSourceUser,
							dataSourcePassword, spec.getCollection(), dataSourceColumnNames, dataSourceHost, dataSourcePort,
							dataSourceUser, job.getId(), String.valueOf(offset), String.valueOf(this.chunkSize),String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
							String.valueOf(this.chunkSize),client,language,systsemNumber);
					chunkCount++;
					CommonUtil.postMessage(queue,contextParams,JOB_NAME,this.job.getId(),DEPENDENCY_JAR,scheduleQueueName,ds.getType(), 
							1,chunkCount);
				
			 }
			 synchronized (this.job) {
					this.job.setChunksCount(chunkCount); 
				}
		} catch (Exception e) {
			synchronized (this.job) {
				this.job.setState(DataExtractionJob.StateEnum.FAILED);
				this.job.setFailureMessage(e.getMessage());
			}

		}
	}

	private static String removeLastChar(String str) {
		return str.substring(0, str.length() - 1);
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
}



