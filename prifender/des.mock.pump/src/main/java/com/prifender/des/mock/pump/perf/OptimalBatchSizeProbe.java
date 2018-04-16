package com.prifender.des.mock.pump.perf;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;

@Component
public final class OptimalBatchSizeProbe
{
    @Autowired
    protected RelationalDatabaseClient database;

    @Autowired
    private List<PerfTest> tests;
    
    @Value( "#{T(java.util.Arrays).asList('${perf.probe.optimalBatchSize.batchSizes}')}" )
    private List<Integer> batchSizes;
    
    private PerfTest test;
    
    public void run( final String insertStrategy ) throws Exception
    {
        for( final PerfTest test : this.tests )
        {
            if( test.name().equals( insertStrategy ) )
            {
                this.test = test;
            }
        }
        
        if( this.test == null )
        {
            System.out.println( "Unknown insert strategy " + insertStrategy );
        }
        
        printHeader();
        
        final int batchSizesCount = this.batchSizes.size();
        final int[] recordsPerSecond = new int[ batchSizesCount ];
        
        for( int i = 0; i < batchSizesCount; i++ )
        {
            this.test.run( batchSizes.get( i ) );
            recordsPerSecond[ i ] = this.test.recordsPerSecond();
        }
        
        printHeader();
        
        System.out.println();
        
        for( int i = 0; i < batchSizesCount; i++ )
        {
            System.out.println( batchSizes.get( i ) + ": " + recordsPerSecond[ i ] + "/sec" );
        }

        System.out.println();
    }
    
    private void printHeader() throws Exception
    {
        System.out.println();
        System.out.println( "Optimal Batch Size Probe" );
        System.out.println();
        System.out.println( "Database: " + this.database.databaseNameVersion() );
        System.out.println( "Connection URL: " + this.database.url() );
        System.out.println( "Table Namespace: " + this.database.namespace() );
        System.out.println( "Insert Strategy: " + this.test.name() );
        System.out.println( "Batch Sizes: " + this.batchSizes.toString() );
    }

}
