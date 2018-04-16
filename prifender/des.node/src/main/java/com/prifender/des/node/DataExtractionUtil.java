package com.prifender.des.node;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataExtractionUtil {
	protected static final Log LOGGER = LogFactory.getLog(DataExtractionUtil.class);

	public static class Temp {
		private Temp() {
		}

		public static final String TEMP_FILE_DIR;
		static {
			TEMP_FILE_DIR = System.getProperty("java.io.tmpdir") + "/Prifender_Des/";
			File tempPath = new File(TEMP_FILE_DIR);
			try {
				if (!tempPath.exists()) {
					tempPath.mkdirs();
				}
			} catch (Exception e) {
				System.out.println("Unable to create temp folder : " + TEMP_FILE_DIR + "; " + e.getMessage());
			}
		}

		public static String getTempFileDir() {
			return TEMP_FILE_DIR;
		}
	}

	final public static class Config {
		private Config() {
		}

		public static final String PRIFENDER_CONFIG_BASE_DIRECTORY_WINDOWS = "C:";
		public static final String PRIFENDER_CONFIG_BASE_DIRECTORY_OTHER = "/usr";
		public static final String PRIFENDER_CONFIG_DIRECTORY = "_PRIFENDER";
		public static final String CONFIG_HOME = getBaseConfigPath();
		public static final String ETLJOBS = CONFIG_HOME + "/ETLJOBS/";
		public static final String DISK_CONFIG = CONFIG_HOME + "Prifender_Des.config";

		public static String getBaseConfigPath() {
			String baseConfigPath = "";
			if (SystemUtils.IS_OS_WINDOWS) {
				baseConfigPath = PRIFENDER_CONFIG_BASE_DIRECTORY_WINDOWS;
			} else if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX || SystemUtils.IS_OS_MAC) {
				baseConfigPath = PRIFENDER_CONFIG_BASE_DIRECTORY_OTHER;
			}
			baseConfigPath += File.separator + PRIFENDER_CONFIG_DIRECTORY + File.separator;
			return baseConfigPath;
		}

	}


	public static String createDir(String baseDir,String dirName) {
		if (StringUtils.isBlank(baseDir)) {
			baseDir = Temp.getTempFileDir();
		}
		if (StringUtils.isNotBlank(dirName)) {
			dirName = baseDir + "/" + dirName+"/";
			if (!new File(dirName).exists()) {
				new File(dirName).mkdirs();
			}
		}
		return dirName;
	}
	
	public static String createDir(String dirName) {
		if (StringUtils.isNotBlank(dirName)) {
			if (!new File(dirName).exists()) {
				new File(dirName).mkdirs();
			}
		}
		return dirName;
	}

	public static void createFile(String path) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
	}
	
}
