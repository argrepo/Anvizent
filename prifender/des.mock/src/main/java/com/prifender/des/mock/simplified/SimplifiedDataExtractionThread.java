package com.prifender.des.mock.simplified;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import com.prifender.des.mock.DataExtractionThread;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

public final class SimplifiedDataExtractionThread extends DataExtractionThread
{
    public SimplifiedDataExtractionThread( final DataExtractionSpec spec, final DataExtractionJob job, final int mockDataSize, final MessagingConnectionFactory messaging )
    {
        super( spec, job, mockDataSize, messaging );
    }
    
    @Override
    public void run()
    {
        final String queueName = "Mock-DES-" + this.job.getId();
        final int maxObjectsToExtract = getMaxObjectsToExtract();

        synchronized( this.job )
        {
            this.job.setState( DataExtractionJob.StateEnum.RUNNING );
            this.job.setTimeStarted( DATE_FORMAT.format( new Date() ) );
            this.job.setOutputMessagingQueue( queueName );
            this.job.setObjectCount( maxObjectsToExtract );
            this.job.setObjectsExtracted( 0 );
        }
        
        try( final MessagingConnection messagingServiceConnection = this.messaging.connect() )
        {
            final MessagingQueue queue = messagingServiceConnection.queue( queueName );
            final SimplifiedDataSource data = new SimplifiedDataSource( this.mockDataSize );
            
            int objectsExtracted = 0;
            
            for( final Iterator<Map<String,Object>> itr = data.table( this.spec.getCollection() ); 
                 itr.hasNext() && ! canceled() && objectsExtracted < maxObjectsToExtract; )
            {
                final Map<String,Object> src = itr.next();
                final JsonObjectBuilder dest = Json.createObjectBuilder();
                
                for( final DataExtractionAttribute attr : this.spec.getAttributes() )
                {
                    final String name = attr.getName();
                    final Object value = src.get( name );
                    
                    if( value instanceof String )
                    {
                        dest.add( name, (String) value );
                    }
                    else if( value instanceof Integer )
                    {
                        dest.add( name, (Integer) value );
                    }
                    else if( value != null )
                    {
                        throw new IllegalStateException( "Value type: " + value.getClass().getName() );
                    }
                }
                
                queue.post( dest.build().toString() );
                
                objectsExtracted++;
                
                synchronized( this.job )
                {
                    this.job.setObjectsExtracted( objectsExtracted );
                }
            }
            
            synchronized( this.job )
            {
                this.job.setState( DataExtractionJob.StateEnum.SUCCEEDED );
            }
        }
        catch( final Exception e )
        {
            e.printStackTrace();
            
            synchronized( this.job )
            {
                this.job.setState( DataExtractionJob.StateEnum.FAILED );
                this.job.setFailureMessage( e.getClass().getSimpleName() + ": " + e.getMessage() );
            }
        }
        
        synchronized( this )
        {
            this.job.setTimeCompleted( DATE_FORMAT.format( new Date() ) );
        }
    }

}
