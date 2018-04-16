package com.prifender.des.mock.hierarchical;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

import com.prifender.des.mock.DataExtractionThread;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

public final class HierarchicalDataExtractionThread extends DataExtractionThread
{
    public HierarchicalDataExtractionThread( final DataExtractionSpec spec, final DataExtractionJob job, final int mockDataSize, final MessagingConnectionFactory messaging )
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
            final HierarchicalDataSource data = new HierarchicalDataSource( this.mockDataSize );
            
            int objectsExtracted = 0;
            
            for( final Iterator<JsonObject> itr = data.iterator(); itr.hasNext() && ! canceled() && objectsExtracted < maxObjectsToExtract; )
            {
                final JsonObject src = itr.next();
                final JsonObjectBuilder dest = Json.createObjectBuilder();
                
                extract( src, this.spec.getAttributes(), dest );
                
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
    
    private static void extract( final JsonObject src, final List<DataExtractionAttribute> spec, final JsonObjectBuilder dest )
    {
        if( spec != null && ! spec.isEmpty() )
        {
            for( final DataExtractionAttribute attrInSpec : spec )
            {
                final String attrName = attrInSpec.getName();
                final JsonValue attrValue = src.get( attrName );
                
                if( attrValue instanceof JsonString )
                {
                    dest.add( attrName, ( (JsonString) attrValue ).getString() );
                }
                else if( attrValue instanceof JsonNumber )
                {
                    dest.add( attrName, ( (JsonNumber) attrValue ).bigDecimalValue() );
                }
                else if( attrValue instanceof JsonObject )
                {
                    final JsonObjectBuilder childObjectBuilder = Json.createObjectBuilder();
                    extract( (JsonObject) attrValue, attrInSpec.getChildren(), childObjectBuilder );
                    dest.add( attrName, childObjectBuilder.build() );
                }
                else if( attrValue instanceof JsonArray )
                {
                    final JsonArrayBuilder childArrayBuilder = Json.createArrayBuilder();
                    extract( (JsonArray) attrValue, attrInSpec.getChildren(), childArrayBuilder );
                    dest.add( attrName, childArrayBuilder.build() );
                }
            }
        }
        else
        {
            for( final Map.Entry<String,JsonValue> entry : src.entrySet() )
            {
                final String attrName = entry.getKey();
                final JsonValue attrValue = entry.getValue();
                
                if( attrValue instanceof JsonString )
                {
                    dest.add( attrName, ( (JsonString) attrValue ).getString() );
                }
                else if( attrValue instanceof JsonNumber )
                {
                    dest.add( attrName, ( (JsonNumber) attrValue ).bigDecimalValue() );
                }
                else if( attrValue instanceof JsonObject )
                {
                    final JsonObjectBuilder childObjectBuilder = Json.createObjectBuilder();
                    extract( (JsonObject) attrValue, null, childObjectBuilder );
                    dest.add( attrName, childObjectBuilder.build() );
                }
                else if( attrValue instanceof JsonArray )
                {
                    final JsonArrayBuilder childArrayBuilder = Json.createArrayBuilder();
                    extract( (JsonArray) attrValue, null, childArrayBuilder );
                    dest.add( attrName, childArrayBuilder.build() );
                }
            }
        }
    }
    
    private static void extract( final JsonArray src, final List<DataExtractionAttribute> spec, final JsonArrayBuilder dest )
    {
        for( final JsonValue entry : src )
        {
            if( entry instanceof JsonString )
            {
                dest.add( ( (JsonString) entry ).getString() );
            }
            else if( entry instanceof JsonNumber )
            {
                dest.add( ( (JsonNumber) entry ).bigDecimalValue() );
            }
            else if( entry instanceof JsonObject )
            {
                final JsonObjectBuilder childObjectBuilder = Json.createObjectBuilder();
                extract( (JsonObject) entry, spec, childObjectBuilder );
                dest.add( childObjectBuilder.build() );
            }
            else if( entry instanceof JsonArray )
            {
                final JsonArrayBuilder childArrayBuilder = Json.createArrayBuilder();
                extract( (JsonArray) entry, spec, childArrayBuilder );
                dest.add( childArrayBuilder.build() );
            }
        }
    }

}
