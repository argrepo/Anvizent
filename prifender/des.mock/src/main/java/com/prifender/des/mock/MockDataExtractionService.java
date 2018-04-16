package com.prifender.des.mock;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.prifender.des.DataExtractionService;
import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.mock.DataSourceAdapter.StartResult;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component( "dataExtractionService" )
public final class MockDataExtractionService extends DataExtractionService
{
    @Autowired
    private List<DataSourceAdapter> adapters;
    
    @Autowired
    private MessagingConnectionFactory messagingConnectionFactory;
    
    private final Map<DataExtractionJob,DataExtractionThread> jobToThreadMap = new IdentityHashMap<>();
    
    @Override
    public List<DataSourceType> getSupportedDataSourceTypes()
    {
        final List<DataSourceType> types = new ArrayList<DataSourceType>( this.adapters.size() );

        this.adapters.forEach( adapter -> types.add( adapter.getDataSourceType() ) );
        
        return types;
    }
    
    private DataSourceAdapter getAdapter( final String type )
    {
        return this.adapters.stream()
                .filter( adapter -> adapter.getDataSourceType().getId().equals( type ) )
                .findFirst()
                .get();
    }

    @Override
    protected ConnectionStatus testConnection( final DataSource ds ) throws DataExtractionServiceException
    {
        return getAdapter( ds.getType() ).testConnection( ds );
    }

    @Override
    protected Metadata getMetadata( final DataSource ds ) throws DataExtractionServiceException
    {
        return getAdapter( ds.getType() ).getMetadata( ds );
    }
    
    @Override
    protected synchronized DataExtractionJob startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec ) throws DataExtractionServiceException
    {
        final DataSourceAdapter adapter = getAdapter( ds.getType() );
        final StartResult result = adapter.startDataExtractionJob( ds, spec, this.messagingConnectionFactory );
        
        this.jobToThreadMap.put( result.job, result.thread );
        
        return result.job;
    }

    @Override
    protected synchronized void deleteDataExtractionJob( final DataExtractionJob job ) throws DataExtractionServiceException
    {
        final DataExtractionThread thread = this.jobToThreadMap.remove( job );
        
        if( thread != null )
        {
            thread.cancel();
        }
    }

}
