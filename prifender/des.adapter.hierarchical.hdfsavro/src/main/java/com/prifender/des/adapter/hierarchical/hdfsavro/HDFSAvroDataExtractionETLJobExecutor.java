package com.prifender.des.adapter.hierarchical.hdfsavro;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.prifender.des.util.DataExtractionETLJobExecution;

public class HDFSAvroDataExtractionETLJobExecutor extends DataExtractionETLJobExecution {

	public HDFSAvroDataExtractionETLJobExecutor(String adapterHome) {
		super(adapterHome);
	}

	public Map<String, String> getContextParams(String jobFilesPath,String jobName,String hdfsUserName,String hdfsFilePath,String dataSourceColumnNames,String dataSourceHost,String dataSourcePort,
			 String jobId ,String offset,String limit,String dataSourceScope, String dataSourceSampleSize
			) throws IOException {
		Map<String, String> ilParamsVals = new LinkedHashMap<>();
 
		ilParamsVals.put("Cont_File",""); 
		ilParamsVals.put("HDFS_URI", dataSourceHost); 
		ilParamsVals.put("HADOOP_USER_NAME", hdfsUserName);
		ilParamsVals.put("HDFS_FILE_PATH", hdfsFilePath);
		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));  
		ilParamsVals.put("FILE_PATH", jobFilesPath);
		ilParamsVals.put("JOB_NAME", jobName);
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		ilParamsVals.put("JOB_ID", jobId);
		ilParamsVals.put("OFFSET", offset);
		ilParamsVals.put("LIMIT", limit);
		ilParamsVals.put("SCOPE", dataSourceScope);
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
		return ilParamsVals;
	  

	}

}
