package com.prifender.des.node;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class ApplicationStartup implements ApplicationListener<ApplicationReadyEvent>{
	@Autowired
	DataExtractionChunkExecution dataExtractionChunkExecution;
	@Override
	public void onApplicationEvent(ApplicationReadyEvent arg0) {
		new Thread(dataExtractionChunkExecution).start();
	}
}
