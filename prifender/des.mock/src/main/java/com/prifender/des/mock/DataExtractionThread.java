package com.prifender.des.mock;

import java.text.DateFormat;

import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.messaging.api.MessagingConnectionFactory;

public abstract class DataExtractionThread extends Thread
{
    protected static final DateFormat DATE_FORMAT = DateFormat.getTimeInstance( DateFormat.DEFAULT );
    
    protected final DataExtractionSpec spec;
    protected final DataExtractionJob job;
    protected final int mockDataSize;
    protected final MessagingConnectionFactory messaging;
    private boolean canceled;
    
    public DataExtractionThread( final DataExtractionSpec spec, final DataExtractionJob job, final int mockDataSize, final MessagingConnectionFactory messaging )
    {
        this.spec = spec;
        this.job = job;
        this.mockDataSize = mockDataSize;
        this.messaging = messaging;
    }
    
    public final DataExtractionJob job()
    {
        return this.job;
    }
    
    public synchronized final void cancel()
    {
        this.canceled = true;
    }
    
    public synchronized final boolean canceled()
    {
        return this.canceled;
    }

    protected final int getMaxObjectsToExtract()
    {
        return getMaxObjectsToExtract( this.mockDataSize );
    }
    
    protected final int getMaxObjectsToExtract( final int sizeOfCollection )
    {
        int max = sizeOfCollection;
        
        if( this.spec.getScope() == DataExtractionSpec.ScopeEnum.SAMPLE )
        {
            final Integer sampleSize = this.spec.getSampleSize();
            
            if( sampleSize != null )
            {
                final int sampleSizeInt = sampleSize;
                
                if( sampleSizeInt > 0 && sampleSizeInt < sizeOfCollection )
                {
                    max = sampleSizeInt;
                }
            }
        }
        
        return max;
    }

}
