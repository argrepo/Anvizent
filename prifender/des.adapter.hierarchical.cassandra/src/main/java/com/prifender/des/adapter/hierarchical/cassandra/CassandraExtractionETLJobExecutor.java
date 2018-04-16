package com.prifender.des.adapter.hierarchical.cassandra;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.prifender.des.util.DataExtractionETLJobExecution;

public class CassandraExtractionETLJobExecutor extends DataExtractionETLJobExecution {

	public CassandraExtractionETLJobExecutor(String adapterHome) {
		super(adapterHome);
	}

	public Map<String, String> getContextParams(String jobFilesPath,String jobName,String dataSourceUser,
			String dataSourcePassword,String dataSourceTableName,String dataSourceColumnNames,String dataSourceHost,String dataSourcePort,
			String dataSourceName ,String jobId ,String offset,String limit,String dataSourceScope, String dataSourceSampleSize
			) throws IOException {
		Map<String, String> ilParamsVals = new LinkedHashMap<>();
 
		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));  
		ilParamsVals.put("FILE_PATH", jobFilesPath);
		ilParamsVals.put("JOB_NAME", jobName);
		ilParamsVals.put("DATASOURCE_USER", dataSourceUser);
		ilParamsVals.put("DATASOURCE_PASS", dataSourcePassword);
		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);
		ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);
		ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
		ilParamsVals.put("JOB_ID", jobId);
		ilParamsVals.put("OFFSET", offset);
		ilParamsVals.put("LIMIT", limit);
		ilParamsVals.put("SCOPE", dataSourceScope);
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);
		return ilParamsVals;

	}

}
