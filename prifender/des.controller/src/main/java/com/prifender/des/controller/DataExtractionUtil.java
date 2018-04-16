package com.prifender.des.controller;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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
