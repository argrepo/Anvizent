package com.prifender.des.mock.hierarchical;

import static com.prifender.des.mock.MockDataExtractionServiceProblems.unknownCollection;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.mock.DataExtractionThread;
import com.prifender.des.mock.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component
public final class HierarchicalDataSourceAdapter extends DataSourceAdapter
{
    private static final String TYPE_ID = "hierarchical.database.mock";
    
    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID )
        .label( "Mock Hierarchical Database" )
        .addConnectionParamsItem
        (
            new ConnectionParamDef()
                .id( "user" )
                .label( "User" )
                .description( "Use mock user 'joe'" )
                .type( TypeEnum.STRING )
        )
        .addConnectionParamsItem
        (
            new ConnectionParamDef()
                .id( "password" )
                .label( "Password" )
                .description( "Use mock password 'somebody'" )
                .type( TypeEnum.PASSWORD )
        )
        .addConnectionParamsItem
        (
            new ConnectionParamDef()
                .id( "size" )
                .label( "Size" )
                .description( "Controls the number of employee records available (default is 1 million)")
                .type( TypeEnum.INTEGER )
                .required( false )
        );
    
    @Override
    public DataSourceType getDataSourceType()
    {
        return TYPE;
    }

    @Override
    public ConnectionStatus testConnection( final DataSource ds ) throws DataExtractionServiceException
    {
        final String user = getConnectionParam( ds, "user" );
        final String password = getConnectionParam( ds, "password" );
        
        if( user.equals( "joe" ) && password.equals( "somebody" ) )
        {
            return new ConnectionStatus().code( ConnectionStatus.CodeEnum.SUCCESS );
        }
        
        return new ConnectionStatus().code( ConnectionStatus.CodeEnum.FAILURE ).message( "Could not connect to mock database" );
    }

    @Override
    public Metadata getMetadata( final DataSource ds ) throws DataExtractionServiceException
    {
        return HierarchicalDataSource.metadata();
    }

    @Override
    public StartResult startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec, final MessagingConnectionFactory messaging ) throws DataExtractionServiceException
    {
        final String collection = spec.getCollection();
        
        if( ! HierarchicalDataSource.isValidCollection( collection ) )
        {
            throw new DataExtractionServiceException( unknownCollection( collection ) );
        }
        
        final DataExtractionJob job 
            = new DataExtractionJob()
                .id( spec.getDataSource() + "-" + collection + "-" + UUID.randomUUID().toString() )
                .state( DataExtractionJob.StateEnum.WAITING );
        
        final DataExtractionThread thread = new HierarchicalDataExtractionThread( spec, job, getMockDataSize( ds ), messaging );
        
        thread.start();
        
        return new StartResult( job, thread );
    }

}
