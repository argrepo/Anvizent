package com.prifender.des.mock.pump.perf;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.prifender.des.mock.pump.Config;
import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;

@Component
public final class OptimalInsertStrategyProbe
{
    @Autowired
    private Config config;

    @Autowired
    protected RelationalDatabaseClient database;

    @Autowired
    private List<PerfTest> tests;
    
    public void run() throws Exception
    {
        sortTestsByName( tests );
        
        printHeader();

        for( final PerfTest test : this.tests )
        {
            try
            {
                test.run();
            }
            catch( final Exception e )
            {
                e.printStackTrace();
            }
        }
        
        printHeader();
        
        System.out.println();
        
        for( final PerfTest test : this.tests )
        {
            System.out.println( test );
        }

        System.out.println();
    }
    
    private void sortTestsByName( final List<PerfTest> tests )
    {
        Collections.sort
        (
            tests,
            new Comparator<PerfTest>()
            {
                @Override
                public int compare( final PerfTest t1, final PerfTest t2)
                {
                    return t1.name().compareTo( t2.name() );
                }
            }
        );
    }
    
    private void printHeader() throws Exception
    {
        System.out.println();
        System.out.println( "Optimal Insert Strategy Probe" );
        System.out.println();
        System.out.println( "Database: " + this.database.databaseNameVersion() );
        System.out.println( "Connection URL: " + this.database.url() );
        System.out.println( "Table Namespace: " + this.database.namespace() );
        System.out.println( "Batch Size: " + this.config.perfBatchSize );
    }

}
