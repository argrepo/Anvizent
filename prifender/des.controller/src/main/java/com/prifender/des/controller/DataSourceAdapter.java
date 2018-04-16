package com.prifender.des.controller;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.messaging.api.MessagingConnectionFactory;

public interface DataSourceAdapter
{
	
	public static final class StartResult
    {
        public final DataExtractionJob job;
        public final DataExtractionThread  thread;
        
        public StartResult( final DataExtractionJob job, final DataExtractionThread thread)
        {
            this.job = job;
            this.thread = thread;
        }
    }
	
    DataSourceType getDataSourceType();

    ConnectionStatus testConnection( DataSource ds ) throws DataExtractionServiceException;

    Metadata getMetadata( DataSource ds ) throws DataExtractionServiceException;
    
    int getCountRows( DataSource ds, String tableName) throws DataExtractionServiceException;
     
    StartResult startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec,final MessagingConnectionFactory messaging) throws DataExtractionServiceException;
  
}
