package com.prifender.des.mock.pump.perf;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.prifender.des.mock.pump.relational.client.OracleClient;

@Component
public final class RegularStatementBatchingBPerfTest extends BatchingPerfTest
{
    @Override
    protected void run( final Connection cn, final String testTableName ) throws Exception
    {
        final int batchSize = batchSize();
        
        for( int i = 0; runAnotherBatch(); i++ )
        {
            final StringBuilder insertQuery = new StringBuilder();
            
            if( this.database instanceof OracleClient )
            {
                insertQuery.append( "BEGIN\n" );
            }
            
            for( int j = 0; j < batchSize; j++ )
            {
                insertQuery
                    .append( "INSERT INTO " )
                    .append( testTableName )
                    .append( " VALUES ( " )
                    .append( i * batchSize + j )
                    .append( ", '" )
                    .append( UUID.randomUUID().toString() )
                    .append( "' );" );
            }
            
            if( this.database instanceof OracleClient )
            {
                insertQuery.append( "END;" );
            }
            
            try( final Statement st = cn.createStatement() )
            {
                st.execute( insertQuery.toString() );
            }
            
            System.out.println( "Inserted " + ( ( i + 1 ) * batchSize )  + " records");
        }
    }

}
