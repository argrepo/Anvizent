package com.prifender.des.mock.pump.perf;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public final class RegularStatementBatchingAPerfTest extends BatchingPerfTest
{
    @Override
    protected void run( final Connection cn, final String testTableName ) throws Exception
    {
        final int batchSize = batchSize();
        
        for( int i = 0; runAnotherBatch(); i++ )
        {
            final StringBuilder insertQuery = new StringBuilder();
            
            insertQuery
                .append( "INSERT INTO " )
                .append( testTableName )
                .append( " VALUES " );

            for( int j = 0; j < batchSize; j++ )
            {
                if( j > 0 )
                {
                    insertQuery.append( ", " );
                }
                
                insertQuery
                    .append( "( " )
                    .append( i * batchSize + j )
                    .append( ", '" )
                    .append( UUID.randomUUID().toString() )
                    .append( "' )" );
            }
            
            try( final Statement st = cn.createStatement() )
            {
                st.execute( insertQuery.toString() );
            }
            
            System.out.println( "Inserted " + ( ( i + 1 ) * batchSize )  + " records");
        }
    }

}
