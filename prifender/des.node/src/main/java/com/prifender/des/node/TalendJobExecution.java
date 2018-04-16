package com.prifender.des.node;

import java.util.Map;

import org.springframework.stereotype.Component;

@Component 
public class TalendJobExecution {
    boolean running = false;
  public boolean  isJobRunning(){
	  return  running;
   }
  Process runEtlJar(String jobName,String dependancyJars,Map<String,String> contextParams,String typeId) throws Exception {
	    Process process=null;
        running = true;
        try{
        	DataExtractionETLJobExecution dataExtractionETLJobExecution = new DataExtractionETLJobExecution(typeId);
        	process = dataExtractionETLJobExecution.runETLjar(jobName,dependancyJars,contextParams);
        	dataExtractionETLJobExecution.runETLjar(process); 
        } catch (Exception e) {
        	throw new  Exception(e.getMessage());
        } finally {
            running = false;
        }
		return process;
    } 
}
