package com.prifender.des.controller;

import java.util.List;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.model.ConnectionParam;
import com.prifender.des.model.ConnectionParamDef;
import com.prifender.des.model.ConnectionParamDef.TypeEnum;
import com.prifender.des.model.ConnectionStatus;
import com.prifender.des.model.DataExtractionJob;
import com.prifender.des.model.DataExtractionSpec;
import com.prifender.des.model.DataExtractionTask;
import com.prifender.des.model.DataSource;
import com.prifender.des.model.DataSourceType;
import com.prifender.des.model.Metadata;

public abstract class DataSourceAdapter
{
    // Host
    
    public static final String PARAM_HOST_ID = "Host";
    public static final String PARAM_HOST_LABEL = "Host";
    public static final String PARAM_HOST_DESCRIPTION = "The domain name or the IP address of the data server";
    
    public static final ConnectionParamDef PARAM_HOST 
        = new ConnectionParamDef().id( PARAM_HOST_ID ).label( PARAM_HOST_LABEL ).description( PARAM_HOST_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Port

    public static final String PARAM_PORT_ID = "Port";
    public static final String PARAM_PORT_LABEL = "Port";
    public static final String PARAM_PORT_DESCRIPTION = "The network port that the data server listens on for connections";
    
    public static final ConnectionParamDef PARAM_PORT 
        = new ConnectionParamDef().id( PARAM_PORT_ID ).label( PARAM_PORT_LABEL ).description( PARAM_PORT_DESCRIPTION ).type( TypeEnum.INTEGER );
    
    // User

    public static final String PARAM_USER_ID = "User";
    public static final String PARAM_USER_LABEL = "User";
    public static final String PARAM_USER_DESCRIPTION = "The user name of the account with permission to access the data";
    
    public static final ConnectionParamDef PARAM_USER 
        = new ConnectionParamDef().id( PARAM_USER_ID ).label( PARAM_USER_LABEL ).description( PARAM_USER_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Password

    public static final String PARAM_PASSWORD_ID = "Password";
    public static final String PARAM_PASSWORD_LABEL = "Password";
    public static final String PARAM_PASSWORD_DESCRIPTION = "The password corresponding to the user with permission to access the data";
    
    public static final ConnectionParamDef PARAM_PASSWORD 
        = new ConnectionParamDef().id( PARAM_PASSWORD_ID ).label( PARAM_PASSWORD_LABEL ).description( PARAM_PASSWORD_DESCRIPTION ).type( TypeEnum.PASSWORD );
    
    // Database

    public static final String PARAM_DATABASE_ID = "Database";
    public static final String PARAM_DATABASE_LABEL = "Database";
    public static final String PARAM_DATABASE_DESCRIPTION = "The name of a database located on the specified host";
    
    public static final ConnectionParamDef PARAM_DATABASE 
        = new ConnectionParamDef().id( PARAM_DATABASE_ID ).label( PARAM_DATABASE_LABEL ).description( PARAM_DATABASE_DESCRIPTION ).type( TypeEnum.STRING );
    
    // Schema

    public static final String PARAM_SCHEMA_ID = "Schema";
    public static final String PARAM_SCHEMA_LABEL = "Schema";
    public static final String PARAM_SCHEMA_DESCRIPTION = "The name of a schema within the specified database";
    
    public static final ConnectionParamDef PARAM_SCHEMA 
        = new ConnectionParamDef().id( PARAM_SCHEMA_ID ).label( PARAM_SCHEMA_LABEL ).description( PARAM_SCHEMA_DESCRIPTION ).type( TypeEnum.STRING );
	
    public static final class StartResult
    {
        public final DataExtractionJob job;
        public final List<DataExtractionTask> dataExtractionTasks;
        
        public StartResult( final DataExtractionJob job,final List<DataExtractionTask> dataExtractionTasks)
        {
            this.job = job;
            this.dataExtractionTasks = dataExtractionTasks;
        }
    }
	
    public abstract DataSourceType getDataSourceType();

    public abstract ConnectionStatus testConnection( DataSource ds ) throws DataExtractionServiceException;

    public abstract Metadata getMetadata( DataSource ds ) throws DataExtractionServiceException;
    
    public abstract StartResult startDataExtractionJob( final DataSource ds, final DataExtractionSpec spec,final int noOfContainers) throws DataExtractionServiceException;
  
    protected static final ConnectionParamDef clone( final ConnectionParamDef param )
    {
        if( param == null )
        {
            throw new IllegalArgumentException();
        }
        
        return new ConnectionParamDef()
            .id( param.getId() )
            .label( param.getLabel() )
            .description( param.getDescription() )
            .type( param.getType() )
            .required( param.getRequired() )
            .defaultValue( param.getDefaultValue() );
    }
  
    public final String getConnectionParam( final DataSource ds, final String param )
    {
        return getConnectionParam( getDataSourceType(), ds, param );
    }

    public static final String getConnectionParam( final DataSourceType dstype, final DataSource ds, final String param )
    {
        if( dstype == null )
        {
            throw new IllegalArgumentException();
        }
        
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
        
        for( final ConnectionParamDef cpdef : dstype.getConnectionParams() )
        {
            if( cpdef.getId().equals( param ) )
            {
                return cpdef.getDefaultValue();
            }
        }
        
        return null;
    }

}
