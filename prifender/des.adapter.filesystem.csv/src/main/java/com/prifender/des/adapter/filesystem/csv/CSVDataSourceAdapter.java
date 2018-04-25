package com.prifender.des.adapter.filesystem.csv;

import static com.prifender.des.util.DatabaseUtil.createDir;
import static com.prifender.des.util.DatabaseUtil.generateTaskSampleSize;
import static com.prifender.des.util.DatabaseUtil.getConvertedDate;
import static com.prifender.des.util.DatabaseUtil.getDataSourceColumnNames;
import static com.prifender.des.util.DatabaseUtil.getDataType;
import static com.prifender.des.util.DatabaseUtil.getUUID;
import static com.prifender.des.util.DatabaseUtil.removeLastChar;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.controller.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Problem;
import com.prifender.des.model.Type;

@Component
public final class CSVDataSourceAdapter extends DataSourceAdapter {

	 
	@Value( "${des.home}" )
	private String desHome;
	
   private static final String JOB_NAME = "local_project.prifender_csv_v1_0_1.Prifender_CSV_v1";
	
	private static final String DEPENDENCY_JAR = "prifender_csv_v1_0_1.jar";
	
	private static final String JOB_NAME_WithoutHeader = "local_project.prifender_csv_without_header_v1_0_1.Prifender_CSV_Without_Header_v1";
	
	private static final String DEPENDENCY_JAR_WithoutHeader = "prifender_csv_without_header_v1_0_1.jar";

	private static final int  MIN_THRESHOULD_ROWS = 100000;		
	
	private static final int  MAX_THRESHOULD_ROWS = 200000;	
	
	public static final String TYPE_ID = "CSV";
	public static final String TYPE_LABEL = "CSV";
	
	  // File

    public static final String PARAM_FILE_ID = "File";
    public static final String PARAM_FILE_LABEL = "File";
    public static final String PARAM_FILE_DESCRIPTION = "The path of the file";
    
    public static final ConnectionParamDef PARAM_FILE 
        = new ConnectionParamDef().id( PARAM_FILE_ID ).label( PARAM_FILE_LABEL ).description( PARAM_FILE_DESCRIPTION ).type( TypeEnum.STRING );
    
	  // Delimiter

    public static final String PARAM_DELIMITER_ID = "Delimiter";
    public static final String PARAM_DELIMITER_LABEL = "Delimiter";
    public static final String PARAM_DELIMITER_DESCRIPTION = "The delimeter of the file";
    
    public static final ConnectionParamDef PARAM_DELIMITER
        = new ConnectionParamDef().id( PARAM_DELIMITER_ID ).label( PARAM_DELIMITER_LABEL ).description( PARAM_DELIMITER_DESCRIPTION ).type( TypeEnum.STRING ).required(false);
    
    // File Regular Expression

    public static final String PARAM_FILE_REG_EXP_ID = "FileRegExp";
    public static final String PARAM_FILE_REG_EXP_LABEL = "File Regular Expression";
    public static final String PARAM_FILE_REG_EXP_DESCRIPTION = "The file regular expression";
    
    public static final ConnectionParamDef PARAM_FILE_REG_EXP
        = new ConnectionParamDef().id( PARAM_FILE_REG_EXP_ID ).label( PARAM_FILE_REG_EXP_LABEL ).description( PARAM_FILE_REG_EXP_DESCRIPTION ).type( TypeEnum.STRING ).required(false);
    
    // Header Exists

    public static final String PARAM_HEADER_EXISTS_ID = "HeaderExists";
    public static final String PARAM_HEADER_EXISTS_LABEL = "Header Exists";
    public static final String PARAM_HEADER_EXISTS_DESCRIPTION = "The headers are exists in file";
    
    public static final ConnectionParamDef PARAM_HEADER_EXISTS
        = new ConnectionParamDef().id( PARAM_HEADER_EXISTS_ID ).label( PARAM_HEADER_EXISTS_LABEL ).description( PARAM_HEADER_EXISTS_DESCRIPTION ).type( TypeEnum.BOOLEAN ).required(false);
    
    
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label(TYPE_LABEL)
			.addConnectionParamsItem(PARAM_FILE)
			.addConnectionParamsItem(PARAM_DELIMITER)
			.addConnectionParamsItem(PARAM_FILE_REG_EXP)
			.addConnectionParamsItem(PARAM_HEADER_EXISTS);

	@Override
	public DataSourceType getDataSourceType() {
		return TYPE;
	}
 
	@Override
	public ConnectionStatus testConnection(DataSource ds) throws DataExtractionServiceException {
		boolean connection;
		connection = getFileSystemExists(ds);
		if (connection) {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.SUCCESS)
					.message("CSV File System connection successfully established.");
		} else {
			return new ConnectionStatus().code(ConnectionStatus.CodeEnum.FAILURE)
					.message("Could not connect to CSV File System");
		}
	}
	
	@Override
	public Metadata getMetadata(DataSource ds) throws DataExtractionServiceException {
		Metadata metadata = null;
		boolean connection ;
		connection = getFileSystemExists(ds);
		final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
 
		metadata = metadataByConnection(connection,fileName,delimiter,csvHeaderExists);
		return metadata;
	}

	private int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {
		
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
 

		int count = 0;
		count = count + getCsvRecordCount(tableName,delimiter,csvHeaderExists);
		
		return count;
	}

	private int getCsvRecordCount(String csvFileName, String delimiter,boolean csvHeaderExists) throws DataExtractionServiceException {
		int count = 0;
		CSVParser csvParser = null;
		List<String> csvFilesList = getAllFiles(csvFileName);
		for (String file : csvFilesList) {
			csvParser = getCsvParser(file, delimiter,csvHeaderExists);
			try {
				List<CSVRecord> csvRecords = csvParser.getRecords();
				count = count + csvRecords.size();
			} catch (IOException e) {
				throw new DataExtractionServiceException(
						new Problem().code("CSV File System Connection Error").message(e.getMessage()));
			}
		}
		return count;
	}
  
	
	private boolean getFileSystemExists(DataSource ds) throws DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String fileName = getConnectionParam(ds, PARAM_FILE_ID);
 
		return getFileName(fileName);
	}

	private boolean getFileName(String fileName) {
		
		boolean fileExists = false;
		File file = new File(fileName);
		
		if (fileName.contains(".csv")) {
			fileExists = file.exists();
		} else {
			File[] filesList = file.listFiles();
			if (filesList == null || filesList.length <= 0) {
				file = new File(fileName + ".csv");
				fileExists = file.exists();
			} else {
				for (File f : filesList) {
					fileExists = f.exists();
				}
			}
		}
		
		return fileExists;
	}

	private Metadata metadataByConnection(boolean con, String fileName, String delimiter,boolean csvHeaderExists)
			throws DataExtractionServiceException {
		Metadata metadata = new Metadata();
		List<String> fileNameList = new ArrayList<>();

		try {
			if (fileNameList.size() <= 0) {
				fileNameList = getAllFiles(fileName);
			}

			boolean exist = false;
			for (String fNme : fileNameList) {
				if (fNme.contains(fileName)) {
					exist = true;
					break;
				}
			}
			if (!exist) {
				throw new DataExtractionServiceException(
						new Problem().code("unknown datasource").message("CSV File not found in the given path."));
			}

			List<NamedType> namedTypeObjectsList = new ArrayList<NamedType>();
			for (String fName : fileNameList) {
				List<NamedType> fNameHeaderList = getFileNameFromList(fName);
				for (NamedType namedType : fNameHeaderList) {
					Type entryType = new Type().kind(Type.KindEnum.OBJECT);
					namedType.getType().setEntryType(entryType);
					List<NamedType> attributeListForColumns = getHeadersFromFile(namedType.getName(), delimiter,csvHeaderExists);
					entryType.setAttributes(attributeListForColumns);
					namedTypeObjectsList.add(namedType);
				}
				metadata.setObjects(namedTypeObjectsList);

			}
		} catch (DataExtractionServiceException e) {
			throw new DataExtractionServiceException(new Problem().code("ERROR").message(e.getMessage()));
		}
		return metadata;
	}

	private List<NamedType> getFileNameFromList(String dataSource) {
		List<NamedType> tableList = new ArrayList<>();
		NamedType namedType = new NamedType();
		namedType.setName(dataSource);
		Type type = new Type().kind(Type.KindEnum.LIST);
		namedType.setType(type);
		tableList.add(namedType);
		return tableList;
	}

	private List<NamedType> getHeadersFromFile(String fName, String delimiter, boolean csvHeaderExists)
			throws DataExtractionServiceException {

		List<NamedType> attributeList = new ArrayList<NamedType>();
		CSVParser csvParser = getCsvParser(fName, delimiter, csvHeaderExists);

		List<CSVRecord> csvRecords = new ArrayList<CSVRecord>();

		try {
			csvRecords = csvParser.getRecords();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Set<String> csvHeaderList = new HashSet<>();
		if (csvHeaderExists) {
			csvHeaderList = csvParser.getHeaderMap().keySet();
		} else {
			if (csvRecords.size() > 0) {
				CSVRecord csvRecord = csvRecords.get(0);
				for (int i = 0; i < csvRecord.size(); i++) {
					csvHeaderList.add("Column_" + (i+1));
				}
			}
		}

		for (String csvHeader : csvHeaderList) {
			NamedType attributeForColumn = new NamedType();
			attributeForColumn.setName(csvHeader.trim());

			String type = "";
			
			if (csvHeaderExists && csvRecords.size() > 0) {
				type = csvRecords.get(0).get(csvHeader).getClass().getSimpleName().toString().toUpperCase();
			} else {
				type = csvHeader.getClass().getSimpleName().toString().toUpperCase();
			}
			
			Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(getDataType(type));
			attributeForColumn.setType(typeForCoumn);
			attributeList.add(attributeForColumn);
		}

		return attributeList;
	}

	private  CSVParser getCsvParser(String fileName,String delimiter,boolean csvHeaderExists) throws DataExtractionServiceException {

		CSVParser csvParser = null;
		CSVFormat csvFileFormat = null;
		String fileDelimiter = "";
		
		try {
			
			FileReader fileReader = new FileReader(fileName);
			fileDelimiter = getDelimiter(fileName, delimiter);
			csvFileFormat = getCsvFormat(fileName,fileDelimiter,csvHeaderExists);
			csvParser = new CSVParser(fileReader, csvFileFormat);
			
		} catch (IOException e) {
			throw new DataExtractionServiceException(new Problem().code("CSV Parser Error").message(e.getMessage()));
		}
		return csvParser;
	}

	private String getDelimiter(String fileName, String delimiter) {
		
		String fileDelimiter = "";
		BufferedReader bufferedReader = null;
		
		if (StringUtils.isNotBlank(delimiter)) {
			fileDelimiter = delimiter;
		} else {
			try {
				bufferedReader = new BufferedReader(new FileReader(fileName));
				fileDelimiter = bufferedReader.readLine();
			} catch (IOException e) {
				e.getMessage();
			} finally {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return fileDelimiter;
	}
	
	
	private CSVFormat getCsvFormat(String fileName, String delimiter,boolean csvHeaderExists) throws DataExtractionServiceException {
		String NEW_LINE_SEPARATOR = "\n";
		CSVFormat csvFileFormat = null;
		
		if (csvHeaderExists) {
			if (delimiter.contains(",")) {
				csvFileFormat = CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withHeader()
						.withRecordSeparator(NEW_LINE_SEPARATOR).withAllowMissingColumnNames(true);
			} else if (delimiter.contains(";")) {
				csvFileFormat = CSVFormat.DEFAULT.withDelimiter(';').withQuote('"').withHeader()
						.withRecordSeparator(NEW_LINE_SEPARATOR).withAllowMissingColumnNames(true);
			} else if (delimiter.contains("|")) {
				csvFileFormat = CSVFormat.INFORMIX_UNLOAD.withHeader().withDelimiter('|').withEscape('\\')
						.withQuote('"').withRecordSeparator("\r\n");
			} else if (delimiter.contains("\t")) {
				csvFileFormat = CSVFormat.MYSQL.withHeader().withDelimiter('\t').withEscape('\\')
						.withIgnoreEmptyLines(false).withQuote(null).withRecordSeparator("\r\n").withNullString("\\N")
						.withQuoteMode(QuoteMode.ALL_NON_NULL);
			} else if (delimiter.contains("=")) {
				csvFileFormat = CSVFormat.POSTGRESQL_TEXT.withHeader().withDelimiter('\t').withEscape('"')
						.withIgnoreEmptyLines(false).withQuote('"').withRecordSeparator("\r\n").withNullString("\\N")
						.withQuoteMode(QuoteMode.ALL_NON_NULL);
			}
		}else{
			if (delimiter.contains(",")) {
				csvFileFormat = CSVFormat.RFC4180.withDelimiter(',').withQuote('"')
						.withRecordSeparator(NEW_LINE_SEPARATOR).withAllowMissingColumnNames(true);
			} else if (delimiter.contains(";")) {
				csvFileFormat = CSVFormat.DEFAULT.withDelimiter(';').withQuote('"')
						.withRecordSeparator(NEW_LINE_SEPARATOR).withAllowMissingColumnNames(true);
			} else if (delimiter.contains("|")) {
				csvFileFormat = CSVFormat.INFORMIX_UNLOAD.withDelimiter('|').withEscape('\\')
						.withQuote('"').withRecordSeparator("\r\n");
			} else if (delimiter.contains("\t")) {
				csvFileFormat = CSVFormat.MYSQL.withDelimiter('\t').withEscape('\\')
						.withIgnoreEmptyLines(false).withQuote(null).withRecordSeparator("\r\n").withNullString("\\N")
						.withQuoteMode(QuoteMode.ALL_NON_NULL);
			} else if (delimiter.contains("=")) {
				csvFileFormat = CSVFormat.POSTGRESQL_TEXT.withDelimiter('\t').withEscape('"')
						.withIgnoreEmptyLines(false).withQuote('"').withRecordSeparator("\r\n").withNullString("\\N")
						.withQuoteMode(QuoteMode.ALL_NON_NULL);
			}
		}

		return csvFileFormat;
	}
	
	private List<String> getAllFiles(String fileName) {
		List<String> fileList = new ArrayList<String>();

		if (fileName.contains(".csv")) {
			fileList.add(fileName);
		} else {
			File file = new File(fileName);
			File[] filesHeaderList = file.listFiles();
			if (filesHeaderList == null || filesHeaderList.length <= 0) {
				fileList.add(fileName + ".csv");
			} else {
				for (File fileHeader : filesHeaderList) {
					fileList.add(fileName + "/" + fileHeader.getName());
				}
			}
		}

		return fileList;
	}
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec ,int containersCount) throws DataExtractionServiceException 
	{
		StartResult startResult = null;
		try {
			
			String fileName = spec.getCollection();
			
			int rowCount = getCountRows(ds, fileName);

			if (rowCount == 0) 
			{
				throw new DataExtractionServiceException( new Problem().code("meta data error").message("No Rows Found in file :" + fileName));
			}
			
			DataExtractionJob job = new DataExtractionJob()
					
					.id(spec.getDataSource() + "-" + new File(fileName).getName() + "-" + getUUID())
					
					.state(DataExtractionJob.StateEnum.WAITING);
 
			String adapterHome = createDir(this.desHome, TYPE_ID);
			
			startResult = new StartResult(job, getDataExtractionTasks(ds, spec, job, rowCount, adapterHome , containersCount));
				
		} 
		catch (Exception err) 
		{
			
			throw new DataExtractionServiceException(new Problem().code("job error").message(err.getMessage()));
		}
		
		return startResult;
	}

	/**
	 * @param ds
	 * @param spec
	 * @param job
	 * @param rowCount
	 * @param adapterHome
	 * @param containersCount
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private List<DataExtractionTask> getDataExtractionTasks(DataSource ds, DataExtractionSpec spec ,
			
			DataExtractionJob job,int rowCount ,String adapterHome , int containersCount) throws DataExtractionServiceException{
		
		int totalSampleSize = 0;
		
		int tasksCount = 0;
		
		List<DataExtractionTask> dataExtractionJobTasks = new ArrayList<DataExtractionTask>();
		
		try
		{
			if ( spec.getScope( ).equals( DataExtractionSpec.ScopeEnum.SAMPLE ) )
			{
				
				if ( spec.getSampleSize( ) == null )
				{
					
					throw new DataExtractionServiceException( new Problem( ).code( "Meta data error" ).message( "sampleSize value not found" ) );
					
				}
				
				totalSampleSize = rowCount < spec.getSampleSize( ) ? rowCount : spec.getSampleSize( );
				
			}else
			{
				totalSampleSize = rowCount;
			}
			
			 synchronized (job) {
	        	   
	        	   job.setOutputMessagingQueue("DES-" + job.getId());
					
	        	   job.objectsExtracted(0);
			 }
			
			if (totalSampleSize <= MIN_THRESHOULD_ROWS ) {
				
				int offset = 1;
				
				dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  totalSampleSize ));
				
				tasksCount++;
				
			} else {
				
				int taskSampleSize = generateTaskSampleSize( totalSampleSize , containersCount );
				
				if ( taskSampleSize <= MIN_THRESHOULD_ROWS ) 
				{  						 
					taskSampleSize = MIN_THRESHOULD_ROWS ;						 
				}
				if ( taskSampleSize > MAX_THRESHOULD_ROWS ) 
				{  						 
					taskSampleSize = MAX_THRESHOULD_ROWS ;						 
				}
				
				int noOfTasks = totalSampleSize / taskSampleSize ;			
				
				int remainingSampleSize = totalSampleSize % taskSampleSize;		
				
				for (int i = 0 ; i < noOfTasks ; i++) 
				{
					
					int offset = taskSampleSize * i + 1;	
					
					dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  taskSampleSize ));
					
					tasksCount++;
				}
				
				if (remainingSampleSize > 0) 
				{								 
					int offset = noOfTasks * taskSampleSize + 1;
					
					dataExtractionJobTasks.add(getDataExtractionTask(  ds,   spec ,  job,  adapterHome,  offset,  remainingSampleSize ));
					
					tasksCount++;
				}
			}
			 
           synchronized (job) 
           {
        	   job.setTasksCount(tasksCount);
        	   
        	   job.setObjectCount(totalSampleSize);
           }

		} catch ( Exception e )
		{
			throw new DataExtractionServiceException( new Problem( ).code( "Error" ).message( e.getMessage() ) ); 
		}
		return dataExtractionJobTasks;
	}

	/**
	 * @param ds
	 * @param spec
	 * @param job
	 * @param adapterHome
	 * @param offset
	 * @param limit
	 * @return
	 * @throws DataExtractionServiceException
	 */
	private final DataExtractionTask getDataExtractionTask(DataSource ds, DataExtractionSpec spec ,
			
			DataExtractionJob job,String adapterHome,int offset,int limit) throws DataExtractionServiceException
	{
		String columnNames = null;
		String jobName = null;
		String dependencyJar = null ;
		DataExtractionTask dataExtractionTask = new DataExtractionTask();
		 
		try
		{
		
		final String fileName = spec.getCollection();
		
		final String delimiter = getConnectionParam(ds, PARAM_DELIMITER_ID);
		
		final boolean csvHeaderExists = Boolean.valueOf(getConnectionParam(ds, PARAM_HEADER_EXISTS_ID));
		
		final String fileRegExp = getConnectionParam(ds, PARAM_FILE_REG_EXP_ID);
		
		
		if(csvHeaderExists){
			columnNames =  getDataSourceColumnNames(ds, spec,".");
		}else{
			String[] columns =  getDataSourceColumnNames(ds, spec,".").split("_");
			columnNames = removeLastChar(columns[1]);
		}
		
		if (csvHeaderExists) {
			jobName = JOB_NAME;
			dependencyJar = DEPENDENCY_JAR;
		} else {
			jobName = JOB_NAME_WithoutHeader;
			dependencyJar = DEPENDENCY_JAR_WithoutHeader;
		}
 
		Map< String , String > contextParams = getContextParams( adapterHome , jobName ,
				
				columnNames , fileName,delimiter, job.getOutputMessagingQueue() ,fileRegExp, String.valueOf(offset) , String.valueOf( limit )  ,
				
				String.valueOf( DataExtractionSpec.ScopeEnum.SAMPLE ) , String.valueOf( limit ) );
		
		dataExtractionTask.taskId("DES-Task-"+ getUUID( ))
						
			                    .jobId( job.getId( ) )
	                           
	                            .typeId( TYPE_ID +"__"+jobName + "__" +dependencyJar )
	                           
	                            .contextParameters( contextParams )
	                           
	                            .numberOfFailedAttempts( 0 );
		}
		catch(Exception e)
		{
			throw new DataExtractionServiceException( new Problem( ).code( "Error" ).message( e.getMessage() ) );
		}
		return dataExtractionTask;
	}
 

	/**
	 * @param jobFilesPath
	 * @param jobName
	 * @param dataSourceColumnNames
	 * @param dataSourceFilePath
	 * @param dataSourceDelimiter
	 * @param jobId
	 * @param fileRegExp
	 * @param offset
	 * @param limit
	 * @param dataSourceScope
	 * @param dataSourceSampleSize
	 * @return
	 * @throws IOException
	 */
	public Map<String, String> getContextParams(String jobFilesPath, String jobName, String dataSourceColumnNames,
			
			String dataSourceFilePath, String dataSourceDelimiter, String jobId,String fileRegExp,String offset, String limit,
			
			String dataSourceScope, String dataSourceSampleSize) throws IOException {

		Map<String, String> ilParamsVals = new LinkedHashMap<>();

		ilParamsVals.put("JOB_STARTDATETIME", getConvertedDate(new Date()));

		ilParamsVals.put("FILE_PATH", jobFilesPath);

		ilParamsVals.put("JOB_NAME", jobName);
		
		ilParamsVals.put("FILEREGEX", fileRegExp);

		ilParamsVals.put("DATASOURCE_COLUMN_NAMES", dataSourceColumnNames);
		
		ilParamsVals.put("DATASOURCE_FILE_PATH", dataSourceFilePath);
		
		ilParamsVals.put("DATASOURCE_DELIMITER", dataSourceDelimiter);

		ilParamsVals.put("JOB_ID", jobId);

		ilParamsVals.put("OFFSET", offset);

		ilParamsVals.put("LIMIT", limit);

		ilParamsVals.put("SCOPE", dataSourceScope);

		ilParamsVals.put("SAMPLESIZE", dataSourceSampleSize);

		return ilParamsVals;

	}
}


