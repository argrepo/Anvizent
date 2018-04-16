package com.prifender.des.mock.pump.perf;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public final class RegularStatementNoBatchingPerfTest extends PerfTest
{
    @Override
    protected void run( final Connection cn, final String testTableName ) throws Exception
    {
        final int batchSize = batchSize();
        
        for( int i = 0; runAnotherBatch(); i++ )
        {
            for( int j = 0; j < batchSize; j++ )
            {
                final StringBuilder insertQuery = new StringBuilder();
                
                insertQuery
                    .append( "INSERT INTO " )
                    .append( testTableName )
                    .append( " VALUES ( " )
                    .append( i * batchSize + j )
                    .append( ", '" )
                    .append( UUID.randomUUID().toString() )
                    .append( "' )" );

                try( final Statement st = cn.createStatement() )
                {
                    st.execute( insertQuery.toString() );
                }
                
                if( j % 100 == 0 && ! ( i == 0 && j == 0 ) )
                {
                    System.out.println( "Inserted " + ( ( i * batchSize ) + j ) + " records");
                }
            }
        }
    }

}
