package com.prifender.des.mock.pump.perf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public final class PreparedStatementNoBatchingPerfTest extends PerfTest
{
    @Override
    protected void run( final Connection cn, final String testTableName ) throws Exception
    {
        final int batchSize = batchSize();
        
        final StringBuilder insertQuery = new StringBuilder();
        insertQuery.append( "INSERT INTO " ).append( testTableName ).append( " VALUES ( ?, ? )" );
        final String insertQueryStr = insertQuery.toString();
        
        try (final PreparedStatement st = cn.prepareStatement(insertQueryStr))
        {
            for( int i = 0; runAnotherBatch(); i++ )
            {
                for( int j = 0; j < batchSize; j++ )
                {
                    st.setInt(1, i * batchSize + j);
                    st.setString(2, UUID.randomUUID().toString());
                    st.execute();
                    
                    if( j % 100 == 0 && ! ( i == 0 && j == 0 ) )
                    {
                        System.out.println( "Inserted " + ( ( i * batchSize ) + j ) + " records");
                    }
                }
            }
        }
    }

}
