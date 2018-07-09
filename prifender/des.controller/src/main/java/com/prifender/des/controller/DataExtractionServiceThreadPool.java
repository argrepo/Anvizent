package com.prifender.des.controller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public final class DataExtractionServiceThreadPool
{
    @Value( "${des.threads}" )
    private int maxThreadCount;

    private ExecutorService executor;
    
    public void execute( final Runnable runnable )
    {
        synchronized( this )
        {
            if( this.executor == null )
            {
                this.executor = Executors.newFixedThreadPool( this.maxThreadCount );
            }
        }
        
        this.executor.execute( runnable );
    }
    
}
