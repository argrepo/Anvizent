package com.prifender.des;

import com.prifender.des.model.Problem;

public final class DataExtractionServiceProblems
{
    public static final Problem unknownDataSourceType( final String type )
    {
        return new Problem()
                .code( "unknownDataSourceType" )
                .message( "Unknown data source type '" + type + "'" );
    }

    public static final Problem unknownDataSource( final String id )
    {
        return new Problem()
                .code( "unknownDataSource" )
                .message( "Unknown data source '" + id + "'" );
    }
    
    public static final Problem unknownDataExtractionJob( final String id )
    {
        return new Problem()
                .code( "unknownDataExtractionJob" )
                .message( "Unknown data extraction job '" + id + "'" );
    }
    
    public static final Problem unknownConnectionParam( final String dataSourceType, final String param )
    {
        return new Problem()
                .code( "unknownConnectionParam" )
                .message( "Unknown connection parameter '" + param + "' for data source type '" + dataSourceType + "'" );
    }

    public static final Problem requiredConnectionParam( final String dataSourceType, final String param )
    {
        return new Problem()
                .code( "requiredConnectionParam" )
                .message( "Connection parameter '" + param + "' is required for data source type '" + dataSourceType + "'" );
    }

    public static final Problem expectedBooleanConnectionParam( final String dataSourceType, final String param, final String value )
    {
        return new Problem()
                .code( "expectedBooleanConnectionParam" )
                .message( "Connection parameter '" + param + "' of data source type '" + dataSourceType + "' is expected to be a boolean, not '" + value + "'" );
    }

    public static final Problem expectedIntegerConnectionParam( final String dataSourceType, final String param, final String value )
    {
        return new Problem()
                .code( "expectedIntegerConnectionParam" )
                .message( "Connection parameter '" + param + "' of data source type '" + dataSourceType + "' is expected to be an integer, not '" + value + "'" );
    }

    public static final Problem connectionParamSpecifiedMultipleTimes( final String param )
    {
        return new Problem()
                .code( "connectionParamSpecifiedMultipleTimes" )
                .message( "Connection parameter '" + param + "' is specified multiple times" );
    }

    public static final Problem couldNotConnectToDataSource( final String param )
    {
        return new Problem()
                .code( "couldNotConnectToDataSource" )
                .message( "Could not connect to data source '" + param + "'" );
    }

    public static final Problem unsupportedOperation()
    {
        return new Problem()
                .code( "unsupportedOperation" )
                .message( "Unsupported operation" );
    }
    
}
