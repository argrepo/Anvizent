package com.prifender.des.controller;

import java.text.DateFormat;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;

public abstract class DataExtractionThread extends Thread
{
    protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance( DateFormat.DEFAULT );
    
    protected final DataExtractionSpec spec;
    protected final DataExtractionJob job;
    protected final int dataSize;
    protected Process etlProcess;
    private boolean canceled;
    
    
    public DataExtractionThread( final DataExtractionSpec spec, final DataExtractionJob job, final int dataSize)
    {
        this.spec = spec;
        this.job = job;
        this.dataSize = dataSize;
    }
    
    public DataExtractionJob job()
    {
        return this.job;
    }
    
    public synchronized void cancel()
    {
        this.canceled = true;
        if (etlProcess != null && etlProcess.isAlive()) {
        	etlProcess.destroy();
        	System.out.println("Job " + job.getId() +" terminated");
        }
    }
    
    public synchronized boolean canceled()
    {
        return this.canceled;
    }

}
