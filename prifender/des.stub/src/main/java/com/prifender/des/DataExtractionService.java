package com.prifender.des;

import static com.prifender.des.DataExtractionServiceProblems.connectionParamSpecifiedMultipleTimes;
import static com.prifender.des.DataExtractionServiceProblems.expectedBooleanConnectionParam;
import static com.prifender.des.DataExtractionServiceProblems.expectedIntegerConnectionParam;
import static com.prifender.des.DataExtractionServiceProblems.requiredConnectionParam;
import static com.prifender.des.DataExtractionServiceProblems.unknownConnectionParam;
import static com.prifender.des.DataExtractionServiceProblems.unknownDataSource;
import static com.prifender.des.DataExtractionServiceProblems.unknownDataSourceType;
import static com.prifender.des.DataExtractionServiceProblems.unsupportedOperation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.web.multipart.MultipartFile;

import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.TransformationDef;
import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.Transformations;

public abstract class DataExtractionService
{
    private final List<DataSource> dataSources = new ArrayList<>();
    private final List<DataExtractionJob> dataExtractionJobs = new ArrayList<>();
    private final Transformations transformations = new Transformations();
    
    public abstract List<DataSourceType> getSupportedDataSourceTypes();
    
    public synchronized DataSourceType getSupportedDataSourceType( final String id )
    {
        for( final DataSourceType dst : getSupportedDataSourceTypes() )
        {
            if( dst.getId().equals( id ) )
            {
                return dst;
            }
        }
        
        return null;
    }
    
    public synchronized List<DataSource> getDataSources()
    {
        return this.dataSources;
    }

    public synchronized DataSource getDataSource( final String id )
    {
        for( final DataSource ds : this.dataSources )
        {
            if( ds.getId().equals( id ) )
            {
                return ds;
            }
        }
        
        return null;
    }

    public synchronized DataSource addDataSource( final DataSource ds ) throws DataExtractionServiceException
    {
        validateDataSource( ds );
        
        this.dataSources.add( ds );
        
        return ds;
    }
    
    public synchronized void updateDataSource( final String id, final DataSource ds ) throws DataExtractionServiceException
    {
        final DataSource current = getDataSource( id );
        
        if( current == null )
        {
            throw new DataExtractionServiceException( unknownDataSource( id ) );
        }
        
        validateDataSource( ds );
        
        ds.setId( id );
        
        for( int i = 0, n = this.dataSources.size(); i < n; i++ )
        {
            if( this.dataSources.get( i ) == current )
            {
                this.dataSources.set( i, ds );
                break;
            }
        }
    }
    
    private void validateDataSource( final DataSource ds ) throws DataExtractionServiceException
    {
        if( ds == null )
        {
            throw new IllegalArgumentException();
        }
        
        final String type = ds.getType();
        final DataSourceType dst = getSupportedDataSourceType( type );
        
        if( dst == null )
        {
            throw new DataExtractionServiceException( unknownDataSourceType( type ) );
        }
        
        final String dstId = dst.getId();
        final String dsIdOriginal = ds.getId();
        
        if( dsIdOriginal != null )
        {
            ds.setId( generateDataSourceId( dsIdOriginal ) );
        }
        else
        {
            ds.setId( generateDataSourceId( ds.getLabel() ) );
        }
        
        final Set<String> specifiedParams = new HashSet<String>();
        
        for( final Iterator<ConnectionParam> itr = ds.getConnectionParams().iterator(); itr.hasNext(); )
        {
            final ConnectionParam cp = itr.next();
            final String cpId = cp.getId();
            final ConnectionParamDef cpDef = getConnectionParamDef( dst, cpId );
            
            if( cpDef == null )
            {
                throw new DataExtractionServiceException( unknownConnectionParam( dstId, cpId ) );
            }
            
            String cpValue = cp.getValue();
            
            if( cpValue != null )
            {
                cpValue = cpValue.trim();
                cp.value( cpValue );
            }
            
            if( cpValue == null || cpValue.length() == 0 )
            {
                if( cpDef.getRequired() )
                {
                    throw new DataExtractionServiceException( requiredConnectionParam( dstId, cpId ) );
                }
                
                itr.remove();
            }
            else
            {
                final ConnectionParamDef.TypeEnum cpType = cpDef.getType();
                
                switch( cpType )
                {
                    case BOOLEAN:
                    {
                        if( ! ( cpValue.equals( "true" ) || cpValue.equals( "false" ) ) )
                        {
                            throw new DataExtractionServiceException( expectedBooleanConnectionParam( dstId, cpId, cpValue ) );
                        }
                        
                        break;
                    }
                    case INTEGER:
                    {
                        try
                        {
                            Integer.parseInt( cpValue );
                        }
                        catch( final NumberFormatException e )
                        {
                            throw new DataExtractionServiceException( expectedIntegerConnectionParam( dstId, cpId, cpValue ) );
                        }
                        
                        break;
                    }
                    default:
                    {
                        // No validation for string variants
                    }
                }
            }
            
            if( specifiedParams.contains( cpId ) )
            {
                throw new DataExtractionServiceException( connectionParamSpecifiedMultipleTimes( cpId ) );
            }
            
            specifiedParams.add( cpId );
        }
        
        for( final ConnectionParamDef cpDef : dst.getConnectionParams() )
        {
            final String cpId = cpDef.getId();
            
            if( cpDef.getRequired() && ! specifiedParams.contains( cpId ) )
            {
                throw new DataExtractionServiceException( requiredConnectionParam( dstId, cpId ) );
            }
        }
    }

    public synchronized void deleteDataSource( final String id )
    {
        final DataSource ds = getDataSource( id );
        
        if( ds != null )
        {
            this.dataSources.remove( ds );
        }
    }
    
    public synchronized ConnectionStatus testConnection( final String id ) throws DataExtractionServiceException
    {
        final DataSource ds = getDataSource( id );
        
        if( ds == null )
        {
            throw new DataExtractionServiceException( unknownDataSource( id ) );
        }
        
        return testConnection( ds );
    }
    
    protected abstract ConnectionStatus testConnection( DataSource ds ) throws DataExtractionServiceException;
    
    public synchronized Metadata getMetadata( final String id ) throws DataExtractionServiceException
    {
        final DataSource ds = getDataSource( id );
        
        if( ds == null )
        {
            throw new DataExtractionServiceException( unknownDataSource( id ) );
        }
        
        return getMetadata( ds );
    }
    
    protected abstract Metadata getMetadata( DataSource ds ) throws DataExtractionServiceException;

    public synchronized List<DataExtractionJob> getDataExtractionJobs()
    {
        return this.dataExtractionJobs;
    }
    
    public synchronized DataExtractionJob getDataExtractionJob( final String id )
    {
        for( final DataExtractionJob job : this.dataExtractionJobs )
        {
            if( job.getId().equals( id ) )
            {
                return job;
            }
        }
        
        return null;
    }

    public final synchronized DataExtractionJob startDataExtractionJob( final DataExtractionSpec spec ) throws DataExtractionServiceException
    {
        if( spec == null )
        {
            throw new IllegalArgumentException();
        }
        
        final String dsId = spec.getDataSource();
        final DataSource ds = getDataSource( dsId );
        
        if( ds == null )
        {
            throw new DataExtractionServiceException( unknownDataSource( dsId ) );
        }
        
        final DataExtractionJob job = startDataExtractionJob( ds, spec );
        
        if( job == null )
        {
            throw new IllegalStateException();
        }
        
        this.dataExtractionJobs.add( job );
        
        return job;
    }
    
    protected abstract DataExtractionJob startDataExtractionJob( DataSource ds, DataExtractionSpec spec ) throws DataExtractionServiceException;
    
    public final synchronized void deleteDataExtractionJob( final String id ) throws DataExtractionServiceException
    {
        final DataExtractionJob job = getDataExtractionJob( id );
        
        if( job != null )
        {
            deleteDataExtractionJob( job );
            this.dataExtractionJobs.remove( job );
        }
    }

    protected abstract void deleteDataExtractionJob( DataExtractionJob job ) throws DataExtractionServiceException;
    
    public final List<TransformationDef> getTransformations() throws DataExtractionServiceException
    {
        final List<TransformationDef> tdefs = new ArrayList<TransformationDef>();
        
        for( final Transformation t : this.transformations.list() )
        {
            tdefs.add( new TransformationDef().name( t.getName() ).description( t.getDescription() ) );
        }
        
        return tdefs;
    }
    
    public TransformationDef addTransformation( final MultipartFile file ) throws DataExtractionServiceException
    {
        throw new DataExtractionServiceException( unsupportedOperation() );
    }
    
    protected static final ConnectionParamDef getConnectionParamDef( final DataSourceType dst, final String param )
    {
        if( dst == null )
        {
            throw new IllegalArgumentException();
        }
        
        if( param == null )
        {
            throw new IllegalArgumentException();
        }
        
        for( final ConnectionParamDef cpd : dst.getConnectionParams() )
        {
            if( cpd.getId().equals( param ) )
            {
                return cpd;
            }
        }
        
        return null;
    }
    
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

    private final String generateDataSourceId( final String label )
    {
        final StringBuilder buf = new StringBuilder();
        final int labelLength = label.length();
        int replacedCharacters = 0;
        
        for( int i = 0; i < labelLength; i++ )
        {
            final char ch = label.charAt( i );
            
            if( ( ch >= '0' && ch <= '9' ) || ( ch >= 'A' && ch <= 'Z' ) || ( ch >= 'a' && ch <= 'z' ) || ch == '-' || ch == '_' || ch == '.' || ch == '~' || ch == '.' )
            {
                buf.append( ch );
            }
            else
            {
                buf.append( '_' );
                replacedCharacters++;
            }
        }
        
        final String base;
        
        if( replacedCharacters > ( labelLength / 2 ) )
        {
            base = UUID.randomUUID().toString();
        }
        else
        {
            base = buf.toString();
        }
        
        String id = base;
        
        for( int i = 1; getDataSource( id ) != null; i++ )
        {
            id = base + "_" + i;
        }
        
        return id;
    }
    
}
