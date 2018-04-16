package com.prifender.des.mock;

import com.prifender.des.model.Problem;

public final class MockDataExtractionServiceProblems
{
    public static final Problem unknownCollection( final String collection )
    {
        return new Problem()
                .code( "unknownCollection" )
                .message( "Unknown collection '" + collection + "'" );
    }

    public static final Problem unknownColumn( final String table, final String column )
    {
        return new Problem()
                .code( "unknownColumn" )
                .message( "Unknown column '" + column + "' for table '" + table + "'" );
    }
    
    public static final Problem invalidDataExtractionSpec()
    {
        return new Problem()
                .code( "invalidDataExtractionSpec" )
                .message( "Invalid data extraction specification" );
    }
    
    public static final Problem transformationsNotSupported()
    {
        return new Problem()
                .code( "transformsNotSupported" )
                .message( "The mock Data Extraction Service does not support transformations" );
    }
    
}
