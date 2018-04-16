package com.prifender.des.mock;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.messaging.api.MessagingConnectionFactory;

public abstract class DataSourceAdapter
{
    public abstract DataSourceType getDataSourceType();

    public abstract ConnectionStatus testConnection( DataSource ds ) throws DataExtractionServiceException;

    public abstract Metadata getMetadata( DataSource ds ) throws DataExtractionServiceException;
    
    public static final class StartResult
    {
        public final DataExtractionJob job;
        public final DataExtractionThread thread;
        
        public StartResult( final DataExtractionJob job, final DataExtractionThread thread )
        {
            this.job = job;
            this.thread = thread;
        }
    }
    
    public abstract StartResult startDataExtractionJob( DataSource ds, DataExtractionSpec spec, MessagingConnectionFactory messaging ) throws DataExtractionServiceException;

    protected static final String getConnectionParam( final DataSource ds, final String param )
    {
        if( ds == null )
        {
            throw new IllegalArgumentException();
        }
        
        if( param == null )
        {
            throw new IllegalArgumentException();
        }
        
        for( final ConnectionParam cp : ds.getConnectionParams() )
        {
            if( cp.getId().equals( param ) )
            {
                return cp.getValue();
            }
        }
        
        return null;
    }
    
    protected static final int getMockDataSize( final DataSource ds )
    {
        final String mockDataSizeString = getConnectionParam( ds, "size" );
        int mockDataSize = 0;
        
        if( mockDataSizeString != null )
        {
            try
            {
                mockDataSize = Integer.parseInt( mockDataSizeString );
            }
            catch( final NumberFormatException e ) {}
        }
        
        if( mockDataSize < 1 )
        {
            mockDataSize = 1000000;
        }
        
        return mockDataSize;
    }
    
}
