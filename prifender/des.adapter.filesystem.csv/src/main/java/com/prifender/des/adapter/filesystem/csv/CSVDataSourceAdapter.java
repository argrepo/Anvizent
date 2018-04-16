package com.prifender.des.adapter.filesystem.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang.StringUtils;
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
import com.prifender.des.model.DataExtractionSpec.ScopeEnum;
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
public final class CSVDataSourceAdapter implements DataSourceAdapter {

	@Autowired
	DatabaseUtil databaseUtilService;
	@Value( "${des.home}" )
	private String desHome;
	private static final String TYPE_ID = "CSV";
	@Autowired
	EncryptionServiceClient encryptionServiceClient;
	private static final DataSourceType TYPE = new DataSourceType().id(TYPE_ID).label("CSV")
			.addConnectionParamsItem(new ConnectionParamDef().id("FileName").label("FileName").type(TypeEnum.STRING))
			.addConnectionParamsItem(new ConnectionParamDef().id("Delimiter").label("Delimiter").type(TypeEnum.STRING).required(false))
			.addConnectionParamsItem(new ConnectionParamDef().id("CSVHeaderExists").label("CSVHeaderExists").type(TypeEnum.BOOLEAN));

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
		final String fileName = databaseUtilService.getConnectionParam(ds, "FileName");
		final String delimiter = databaseUtilService.getConnectionParam(ds, "Delimiter");
		final boolean csvHeaderExists = Boolean.valueOf(databaseUtilService.getConnectionParam(ds, "CSVHeaderExists"));
		metadata = metadataByConnection(connection,fileName,delimiter,csvHeaderExists);
		return metadata;
	}

	@Override
	public int getCountRows(DataSource ds, String tableName) throws DataExtractionServiceException {

		final String delimiter = databaseUtilService.getConnectionParam(ds, "Delimiter");
		final boolean csvHeaderExists = Boolean.valueOf(databaseUtilService.getConnectionParam(ds, "CSVHeaderExists"));

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
	
	@Override
	public StartResult startDataExtractionJob(DataSource ds, DataExtractionSpec spec,final MessagingConnectionFactory messaging) throws DataExtractionServiceException {
		StartResult startResult = null;
		try {
			
			String fileName = spec.getCollection();
			int dataSize = getCountRows(ds, fileName);
			
			if (dataSize == 0) {
				throw new DataExtractionServiceException(
						new Problem().code("meta data error").message("No Data in the file :" + fileName));
			}
			
			final String delimiter = databaseUtilService.getConnectionParam(ds, "Delimiter");
			final boolean csvHeaderExists = Boolean.valueOf(databaseUtilService.getConnectionParam(ds, "CSVHeaderExists"));
			
			DataExtractionJob job = new DataExtractionJob()
					.id(spec.getDataSource() + "-" + UUID.randomUUID().toString())
					.state(DataExtractionJob.StateEnum.WAITING);
			ExecutorService executor = Executors.newSingleThreadExecutor();
			try {
				String adapterHome = DataExtractionUtil.createDir(this.desHome, TYPE_ID);
				DataExtractionThread dataExtractionExecutor = new CSVDataExtractionExecutor(ds, spec, job, dataSize, adapterHome, fileName, delimiter,csvHeaderExists,messaging,encryptionServiceClient);
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
	
	private boolean getFileSystemExists(DataSource ds) throws DataExtractionServiceException {
		if (ds == null) {
			throw new DataExtractionServiceException(
					new Problem().code("datasource error").message("datasource is null"));
		}
		final String fileName = databaseUtilService.getConnectionParam(ds, "FileName");
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
			
			Type typeForCoumn = new Type().kind(Type.KindEnum.VALUE).dataType(databaseUtilService.getDataType(type));
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

}


