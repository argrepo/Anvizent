package com.prifender.des.mock.pump;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Value( "${mock.des.url}" )
    public String mockDesUrl;

    @Value( "${mock.data.size}" )
    public int mockDataSize;

    @Value( "${mock.data.minimal}" )
    public boolean mockDataMinimal;

    @Value( "${perf.parallel.processes}" )
    public int perfParallelProcesses;

    @Value( "${perf.batch.size}" )
    public int perfBatchSize;
    
}
