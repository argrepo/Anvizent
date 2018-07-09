package com.prifender.des.controller;

import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.encryption.api.Encryption;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

public final class DataExtractionContext
{
    public final DataSourceAdapter adapter;
    public final DataSourceType dsType;
    public final DataSource ds;
    public final DataExtractionSpec spec;
    public final DataExtractionJob job;
    public final MessagingConnectionFactory messaging;
    public final MessagingQueue pendingTasksQueue;
    public final String pendingTasksQueueName;
    public final String typeId;
    public final Encryption encryption;
    
    public DataExtractionContext( final DataSourceAdapter adapter,
                                  final DataSourceType dsType,
                                  final DataSource ds, 
                                  final DataExtractionSpec spec, 
                                  final DataExtractionJob job, 
                                  final MessagingConnectionFactory messaging ,
                                  final MessagingQueue pendingTasksQueue ,
                                  String pendingTasksQueueName ,
                                  String typeId,
                                  Encryption encryption
    		 )
    {
        this.adapter = adapter;
        this.dsType = dsType;
        this.ds = ds;
        this.spec = spec;
        this.job = job;
        this.messaging = messaging;
        this.pendingTasksQueue=pendingTasksQueue;
        this.pendingTasksQueueName = pendingTasksQueueName;
        this.typeId=typeId;
        this.encryption=encryption;
    }

}
