
package com.prifender.des.adapter.filesystem.csv;

import java.util.Date;
import java.util.Map;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataExtractionThread;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionSpec.ScopeEnum;
import com.prifender.encryption.service.client.EncryptionServiceClient;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.Problem;
import com.prifender.des.util.CommonUtil;

public class CSVDataExtractionExecutor extends DataExtractionThread {

	private static CSVDataExtractionETLJobExecutor etlJobUtil;
	
	private static final String JOB_NAME = "local_project.prifender_csv_v1_0_1.Prifender_CSV_v1";
	private static final String DEPENDENCY_JAR = "prifender_csv_v1_0_1.jar";
	private static final String JOB_NAME_WithoutHeader = "local_project.prifender_csv_without_header_v1_0_1.Prifender_CSV_Without_Header_v1";
	private static final String DEPENDENCY_JAR_WithoutHeader = "prifender_csv_without_header_v1_0_1.jar";

	DataSource ds;
	int chunkSize;
	String adapterHome;
	private ScopeEnum scope;
	String dataSourceFileName;
	String dataSourceDelimiter;
	boolean csvHeaderExists;
	MessagingConnectionFactory messaging;
	EncryptionServiceClient encryptionServiceClient;
	public CSVDataExtractionExecutor (DataSource ds, final DataExtractionSpec spec, final DataExtractionJob job,
			final int dataSize,String adapterHome,String fileName,String delimiter,boolean csvHeaderExists,MessagingConnectionFactory messaging,EncryptionServiceClient encryptionServiceClient) throws DataExtractionServiceException {
		super(spec, job, dataSize);
		etlJobUtil = new CSVDataExtractionETLJobExecutor(adapterHome);
		this.ds = ds;
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
		this.dataSourceFileName = fileName;
		this.dataSourceDelimiter = delimiter;
		this.csvHeaderExists = csvHeaderExists;
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
				stringBuffer.append(attribute.getName()).append(",");
			}
			String dataSourceColumnNames = null;
			if(csvHeaderExists){
				dataSourceColumnNames = removeLastChar(stringBuffer.toString());
			}else{
				String[] spt = stringBuffer.toString().split("_");
				dataSourceColumnNames = removeLastChar(spt[1]);
			}
			
			String jobName = null;
			String dependencyJar = null;
			if (csvHeaderExists) {
				jobName = JOB_NAME;
				dependencyJar = DEPENDENCY_JAR;
			} else {
				jobName = JOB_NAME_WithoutHeader;
				dependencyJar = DEPENDENCY_JAR_WithoutHeader;
			}
			
			int chunkCount = 0;
			 if(this.scope.equals(DataExtractionSpec.ScopeEnum.ALL)){
				   int sampleSizeForChunk = CommonUtil.getSampleSize(this.chunkSize);
				   int calculatedChunkSize = (this.chunkSize / sampleSizeForChunk ); 
					if(calculatedChunkSize > 1){
						for(int i=0;i<calculatedChunkSize;i++){
							int offset = Integer.valueOf(sampleSizeForChunk*i);
						Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, jobName,
								dataSourceColumnNames, dataSourceFileName, dataSourceDelimiter, queueName,
								String.valueOf(offset), String.valueOf(sampleSizeForChunk), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
								String.valueOf(sampleSizeForChunk));
						chunkCount++;
						CommonUtil.postMessage(queue,contextParams,jobName,this.job.getId(),dependencyJar,scheduleQueueName,ds.getType(), 
								calculatedChunkSize,chunkCount);
					}
				}else{
					int offset=0;
					Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, jobName,
							dataSourceColumnNames, dataSourceFileName, dataSourceDelimiter, queueName,
							String.valueOf(offset), String.valueOf(this.chunkSize), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
							String.valueOf(this.chunkSize));
					   chunkCount++;
					CommonUtil.postMessage(queue,contextParams,jobName,this.job.getId(),dependencyJar,scheduleQueueName,ds.getType(),
							1,chunkCount);
				
				}
			 }else{
				 int offset=0;
					Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, jobName,
							dataSourceColumnNames, dataSourceFileName, dataSourceDelimiter, queueName,
							String.valueOf(offset), String.valueOf(this.chunkSize), String.valueOf(DataExtractionSpec.ScopeEnum.SAMPLE),
							String.valueOf(this.chunkSize));
					chunkCount++;
					CommonUtil.postMessage(queue,contextParams,jobName,this.job.getId(),dependencyJar,scheduleQueueName,ds.getType(), 
							1,chunkCount);
				
			 }
			 synchronized (this.job) {
					this.job.setChunksCount(chunkCount); 
				}
			
				/*Map<String, String> contextParams = etlJobUtil.getContextParams(jobFilesPath, jobName,
						dataSourceColumnNames, dataSourceFileName, dataSourceDelimiter, queueName,
						String.valueOf(offset), String.valueOf(chunkSize), this.scope.toString(),
						String.valueOf(chunkSize));
				etlProcess = etlJobUtil.runETLjar(jobName, dependencyJar, contextParams);
				System.out.println(contextParams);
				etlJobUtil.runETLjar(etlProcess);
			
			synchronized (this.job) {
				this.job.setObjectsExtracted(chunkSize);
				this.job.setState(DataExtractionJob.StateEnum.SUCCEEDED);
				this.job.setTimeCompleted(DATE_FORMAT.format(new Date()));
			}*/
		} catch (Exception e) {
			synchronized (this.job) {
				this.job.setState(DataExtractionJob.StateEnum.FAILED);
				this.job.setFailureMessage(e.getMessage());
			}

		}
	}

	private String removeLastChar(String str) {
		return str.substring(0, str.length() - 1);
	}

	protected final String getConnectionParam(final DataSource ds, final String param) {
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
