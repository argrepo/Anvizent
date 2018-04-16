package com.prifender.des.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.prifender.des.controller.DataExtractionStreamGobbler;

public class DataExtractionETLJobExecution {

	private static final String PATH_SEPERATOR = System.getProperty("path.separator");
    private final String adapterHome;
	private final String dependenciesFolder;
	DataExtractionStreamGobbler errorStreamGobbler = null;
	DataExtractionStreamGobbler inputStreamGobbler = null;
	
	public DataExtractionETLJobExecution(final String adapterHome) {
        this.adapterHome = adapterHome;
		this.dependenciesFolder = this.adapterHome + "/COMMON_ETLJOBS";
	}

	public void parseContextParams(final Map<String, String> contextParams, final Map<String, String> paramsVals) {

		Set<Map.Entry<String, String>> set = contextParams.entrySet();
		for (Map.Entry<String, String> entry : set) {
			String paramval = entry.getValue();
			if (paramval.indexOf("{") != -1) {
				int endindex = paramval.indexOf("}");
				String key = paramval.substring(1, endindex);
				String value = paramsVals.get(key);
				if (value != null) {
					entry.setValue(paramval.replace("{" + key + "}", value));
				} else
					System.out.println(key + " --> " + paramval + " --> " + value);
			}
		}
	}
	

	public static String[] convertToContextParamsArray(Map<String, String> params) {
		if (params == null || params.size() == 0)
			return new String[0];
		String[] parameters = new String[params.size()];
		Set<Map.Entry<String, String>> set = params.entrySet();
		int index = 0;
		for (Map.Entry<String, String> entry : set) {
			String param = "--context_param " + entry.getKey() + "=" + entry.getValue();
			parameters[index] = param;
			index++;
		}
		return parameters;
	}

	public Process runETLjar(String jobFileName, String dependencyJARs, Map<String, String> params) throws InterruptedException, IOException {
		return runETLjar(jobFileName, dependencyJARs, convertToContextParamsArray(params));
	}
	
	public Process runETLjar(String jobFileName, String dependencyJARs, String[] ilContextParamsArr)
			throws InterruptedException, IOException {
		String[] commandforExecution = getETLExecCommand(dependencyJARs, jobFileName, ilContextParamsArr);
		Process proc;
		try {
			proc = Runtime.getRuntime().exec(commandforExecution);
		} catch (NullPointerException e) {
			throw new InterruptedException("Job name found null");
		}
		errorStreamGobbler = new DataExtractionStreamGobbler(proc.getErrorStream());
		inputStreamGobbler = new DataExtractionStreamGobbler(proc.getInputStream());
		inputStreamGobbler.start();
		errorStreamGobbler.start();
		
		String errorStreamMsg = errorStreamGobbler.getOutput();

		if (StringUtils.isNotBlank(errorStreamMsg)
				&& errorStreamMsg.contains("Could not find or load main class local_project")) {
			throw new InterruptedException(errorStreamMsg);
		}
		return proc;
	}
	

	public void runETLjar(Process process)
			throws InterruptedException, IOException {

		int exitValue = 1;
		
		if ( process != null ) {
			if (process.isAlive()) {
				exitValue = process.waitFor();
			}
			exitValue = process.exitValue();
		}
		String errorStreamMsg = errorStreamGobbler.getOutput() + "" + inputStreamGobbler.getOutput();

		if (StringUtils.isNotBlank(errorStreamMsg)
				&& errorStreamMsg.contains("Could not find or load main class local_project")) {
			throw new InterruptedException(errorStreamMsg);
		}
		if (exitValue == 1) {
			throw new InterruptedException(errorStreamMsg);
		}
	}

	public String[] getETLExecCommand(String dependentJars, String jobMainClass, String[] contextparmsArray) {
		ArrayList<String> list = new ArrayList<>();
		list.add("java");
		list.add("-cp");
		
		String commonlib = commonETLLib(dependenciesFolder);
		StringBuilder sb = new StringBuilder();
		sb.append(commonlib);

		String ildependentjars = dependentJars;
		String[] il_jars_array = ildependentjars.split(",");

		for (int i = 0; i < il_jars_array.length; i++) {
			String il_jar = il_jars_array[i];
			sb.append(adapterHome + "/" + il_jar);
			if (i < il_jars_array.length - 1) {
				sb.append(PATH_SEPERATOR);
			}
		}

		String jars = sb.toString();
		list.add(jars);
		list.add(jobMainClass);

		for (int i = 0; i < contextparmsArray.length; i++) {
			list.add(contextparmsArray[i]);
		}

		String[] command = new String[list.size()];
		command = list.toArray(command);
		return command;
	}

	public String commonETLLib(String dir) {
		File[] files = getFiles(dir, "jar");
		StringBuilder sb = new StringBuilder();

		for (File file : files) {
			sb.append(file.getAbsolutePath());
			sb.append(PATH_SEPERATOR);
		}

		return sb.toString();
	}

	public File[] getFiles(String dir, final String fileExtn) {
		File[] files = null;
		if (StringUtils.isNotBlank(dir)) {
			File fileDir = new File(dir);
			if (!fileDir.exists()) {
				fileDir.mkdirs();
				System.out.println("dir is created.." + dir);
			} else {
				files = fileDir.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File fileDir, String name) {
						return name.endsWith(fileExtn);
					}
				});

			}
		}

		return files;
	}

	public String getConvertedDate(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
		return formatter.format(date);
	}

}
