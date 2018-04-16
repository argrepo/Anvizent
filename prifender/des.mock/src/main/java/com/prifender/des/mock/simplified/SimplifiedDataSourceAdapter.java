package com.prifender.des.mock.simplified;

import static com.prifender.des.mock.MockDataExtractionServiceProblems.invalidDataExtractionSpec;
import static com.prifender.des.mock.MockDataExtractionServiceProblems.transformationsNotSupported;
import static com.prifender.des.mock.MockDataExtractionServiceProblems.unknownCollection;
import static com.prifender.des.mock.MockDataExtractionServiceProblems.unknownColumn;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.mock.DataSourceAdapter;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionAttribute;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.DataTransformation;
import com.prifender.des.model.Metadata;
import com.prifender.messaging.api.MessagingConnectionFactory;

@Component
public final class SimplifiedDataSourceAdapter extends DataSourceAdapter
{
    private static final String TYPE_ID = "simplified.database.mock";
    
    private static final DataSourceType TYPE = new DataSourceType()
        .id( TYPE_ID )
        .label( "Mock Simplified Database" )
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
        return SimplifiedDataSource.metadata();
    }

    @Override
    public StartResult startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec, final MessagingConnectionFactory messaging ) throws DataExtractionServiceException
    {
        final String tableName = spec.getCollection();
        
        if( ! SimplifiedDataSource.isValidTable( tableName ) )
        {
            throw new DataExtractionServiceException( unknownCollection( tableName ) );
        }
        
        for( final DataExtractionAttribute attribute : spec.getAttributes() )
        {
            final String column = attribute.getName();
            
            if( ! SimplifiedDataSource.isValidColumn( tableName, column ) )
            {
                throw new DataExtractionServiceException( unknownColumn( tableName, column ) );
            }
            
            final List<DataExtractionAttribute> children = attribute.getChildren();
            
            if( ! ( children == null || children.isEmpty() ) )
            {
                throw new DataExtractionServiceException( invalidDataExtractionSpec() );
            }
            
            final List<DataTransformation> transformations = attribute.getTransformations();
            
            if( ! ( transformations == null || transformations.isEmpty() ) )
            {
                throw new DataExtractionServiceException( transformationsNotSupported() );
            }
        }
        
        final DataExtractionJob job 
            = new DataExtractionJob()
                .id( spec.getDataSource() + "-" + tableName + "-" + UUID.randomUUID().toString() )
                .state( DataExtractionJob.StateEnum.WAITING );
        
        final SimplifiedDataExtractionThread thread = new SimplifiedDataExtractionThread( spec, job, getMockDataSize( ds ), messaging );
        
        thread.start();
        
        return new StartResult( job, thread );
    }

}
