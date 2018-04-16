package com.prifender.des.adapter.relational.sqlserverkerbores;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.prifender.des.util.DataExtractionETLJobExecution;

public class SqlServerKerboresDataExtractionETLJobExecutor extends DataExtractionETLJobExecution {

	public SqlServerKerboresDataExtractionETLJobExecutor(String adapterHome) {
		super(adapterHome);
	}

	public Map<String, String> getContextParams(String jobFilesPath, String jobName, String fdqn,
			String realm, String dataSourceTableName, String dataSourceColumnNames, String dataSourceHost,String adminHostName,
			String dataSourcePort, String dataSourceName, String instanceName,String userName,String password, String jobId,String integratedSecurity,String authenticationScheme ,
			String encryptionMethod , String offset, String limit, String dataSourceScope, String dataSourceSampleSize) throws IOException {
		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));
		ilParamsVals.put("FILE_PATH", jobFilesPath);
		ilParamsVals.put("JOB_NAME", jobName);
		ilParamsVals.put("FQDN", fdqn);
		ilParamsVals.put("REALM", realm);
		ilParamsVals.put("DATASOURCE_TABLE_NAME", dataSourceTableName);
		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		ilParamsVals.put("DATASOURCE_HOST", dataSourceHost);
		ilParamsVals.put("ADMINHOSTNAME", adminHostName);
		ilParamsVals.put("DATASOURCE_PORT", dataSourcePort);
		ilParamsVals.put("INSTANCE_NAME", instanceName);
		ilParamsVals.put("DATASOURCE_USER", userName);
		ilParamsVals.put("DATASOURCE_PASS", password);
		ilParamsVals.put("DATASOURCE_NAME", dataSourceName);
		ilParamsVals.put("INTEGRATEDSECURITY", integratedSecurity);
		ilParamsVals.put("AUTHENTICATIONSCHEME", authenticationScheme);
		ilParamsVals.put("ENCRYPTIONMETHOD", encryptionMethod);
		ilParamsVals.put("JOB_ID", jobId);
		ilParamsVals.put("OFFSET", offset);
		ilParamsVals.put("LIMIT", limit);
		ilParamsVals.put("SCOPE", dataSourceScope);
		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		return ilParamsVals;

	}

}
