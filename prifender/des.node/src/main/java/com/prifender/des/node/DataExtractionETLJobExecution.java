package com.prifender.des.node;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class DataExtractionETLJobExecution {

	private static final String PATH_SEPARATOR = System.getProperty("path.separator");
	private static final String File_SEPARATOR = System.getProperty("file.separator");
	
	private final String commonEtlJobsFolder;
	private final String etlJobsFolder;
	DataExtractionStreamGobbler errorStreamGobbler = null;
	DataExtractionStreamGobbler inputStreamGobbler = null;
	
	public DataExtractionETLJobExecution( final String talendJobsPath, final String typeId ) {
		this.commonEtlJobsFolder = talendJobsPath + File_SEPARATOR + typeId + File_SEPARATOR + "COMMON_ETLJOBS";
		this.etlJobsFolder = talendJobsPath + File_SEPARATOR + typeId ;
	}

	public void runETLjar(String jobFileName, String dependencyJARs, Map<String, String> params) throws InterruptedException, IOException {
		String[] commandforExecution = getETLExecCommand(dependencyJARs, jobFileName, convertToContextParamsArray(params));
		Process process;
		try {
		    process = Runtime.getRuntime().exec(commandforExecution);
		} catch (NullPointerException e) {
			throw new InterruptedException("Job name found null");
		}
		errorStreamGobbler = new DataExtractionStreamGobbler(process.getErrorStream());
		inputStreamGobbler = new DataExtractionStreamGobbler(process.getInputStream());
		inputStreamGobbler.start();
		errorStreamGobbler.start();
		
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

	private String[] getETLExecCommand(String dependentJars, String jobMainClass, String[] contextparmsArray) {
		ArrayList<String> list = new ArrayList<String>();
		list.add("java");
		list.add("-cp");

		String commonlib = commonETLLib(commonEtlJobsFolder);
		StringBuilder sb = new StringBuilder();
		sb.append(commonlib);

		String ildependentjars = dependentJars;
		String[] il_jars_array = ildependentjars.split(",");

		for (int i = 0; i < il_jars_array.length; i++) {
			String il_jar = il_jars_array[i];
			sb.append(etlJobsFolder + "/" + il_jar);
			if (i < il_jars_array.length - 1) {
				sb.append(PATH_SEPARATOR);
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
		System.out.println("Etl Jobs command --> ");
		for (String commondLine : list) {
			System.out.println(commondLine);
		}
		return command;
	}

	private static String[] convertToContextParamsArray(Map<String, String> params) {
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

    private static String commonETLLib(String dir) {
		File[] files = getFiles(dir, "jar");
		StringBuilder sb = new StringBuilder();

		for (File file : files) {
			sb.append(file.getAbsolutePath());
			sb.append(PATH_SEPARATOR);
		}

		return sb.toString();
	}

	private static File[] getFiles(String dir, final String fileExtn) {
		File[] files = null;
		if (StringUtils.isNotBlank(dir)) {
			File fileDir = new File(dir);
			if (!fileDir.exists()) {
				fileDir.mkdirs();
				System.out.println("dir is created.." + dir);
			} else {
				files = fileDir.listFiles(new FilenameFilter() {
					public boolean accept(File fileDir, String name) {
						return name.endsWith(fileExtn);
					}
				});
			}
		}

		return files;
	}

}
