package com.prifender.des.mock.pump.perf;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;

import com.prifender.des.mock.pump.Config;
import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;

public abstract class PerfTest
{
    @Autowired
    protected Config config;
    
    @Autowired
    protected RelationalDatabaseClient database;

    private final String name;
    private final boolean enabled;
    private boolean failed;
    private long startTime;
    private long endTime;
    private int batches;
    private int batchSize;
    
    public PerfTest()
    {
        final String baseClassSimpleName = PerfTest.class.getSimpleName();
        final String simpleClassName = getClass().getSimpleName();
        
        if( simpleClassName.endsWith( baseClassSimpleName ) )
        {
            this.name = simpleClassName.substring( 0, simpleClassName.length() - baseClassSimpleName.length() );
        }
        else
        {
            this.name = simpleClassName;
        }

        final String enabledSysProp = System.getProperty( "perf.test." + this.name );
        
        if( enabledSysProp == null )
        {
            this.enabled = true;
        }
        else
        {
            this.enabled = Boolean.valueOf( enabledSysProp );
        }
    }
    
    public final String name()
    {
        return this.name;
    }
    
    public String label()
    {
        return this.name;
    }
    
    public final void run() throws Exception
    {
        run( this.config.perfBatchSize );
    }
    
    public final void run( final int batchSize ) throws Exception
    {
        this.failed = false;
        this.startTime = 0;
        this.endTime = 0;
        this.batches = 0;
        this.batchSize = batchSize;

        if( this.enabled )
        {
            System.out.println();
            System.out.println( label() );
            
            try (final Connection cn = this.database.connect()) {
                
                final String testTableName = this.database.namespace() + ".Test" + System.currentTimeMillis();
                createTestTable( cn, testTableName );
                
                this.startTime = System.currentTimeMillis();
                run( cn, testTableName );
                this.endTime = System.currentTimeMillis();
                
                verifyCount( cn, testTableName );
                deleteTable( cn, testTableName );
            }
            catch( final Exception e )
            {
                e.printStackTrace();
                this.failed = true;
            }
            
            System.out.println();
            System.out.println( toString() );
        }
    }
    
    protected abstract void run( Connection cn, String testTableName ) throws Exception;
    
    private static void createTestTable( final Connection cn, final String tableName ) throws Exception
    {
        final StringBuilder buf = new StringBuilder();

        buf.append("CREATE TABLE ").append(tableName).append(" ( ");
        buf.append("id INTEGER NOT NULL, ");
        buf.append("value VARCHAR(100), ");
        buf.append("PRIMARY KEY ( id ) ");
        buf.append(")");
        
        try( final Statement st = cn.createStatement() )
        {
            st.execute( buf.toString() );
        }
        
        System.out.println( "Created test table " + tableName );
    }
    
    private static void deleteTable( final Connection cn, final String tableName ) throws Exception
    {
        final StringBuilder buf = new StringBuilder();

        buf.append("DROP TABLE ").append(tableName);
        
        try( final Statement st = cn.createStatement() )
        {
            st.execute( buf.toString() );
        }
        
        System.out.println( "Dropped test table " + tableName );
    }
    
    private void verifyCount( final Connection cn, final String tableName ) throws Exception
    {
        try (final Statement st = cn.createStatement()) {
            
            final ResultSet rs = st.executeQuery("select count(*) from " + tableName );
            
            rs.next();
            
            final int count = rs.getInt(1);
            final int expectedCount = totalSize();
            
            if( count == expectedCount )
            {
                System.out.println( "Verified that the test table contains the expected number of records (" + expectedCount + ")" );
            }
            else
            {
                System.out.println( "Expecting test table to contain " + expectedCount + " records, but found " + count + " records" );
                System.exit(1);
            }
        }
    }
    
    protected final int batchSize()
    {
        return this.batchSize;
    }
    
    protected final boolean runAnotherBatch()
    {
        final long timeElapsed = System.currentTimeMillis() - this.startTime;
        
        if( timeElapsed < ( 2 * 60 * 1000 ) )
        {
            this.batches++;
            return true;
        }
        
        return false;
    }
    
    private final int totalSize()
    {
        return this.batches * batchSize();
    }
    
    public final int recordsPerSecond()
    {
        if( this.startTime == 0 || this.endTime == 0 )
        {
            return 0;
        }
        
        return (int) ( ( (double) totalSize() ) / ( this.endTime - this.startTime ) * 1000 );
    }
    
    @Override
    public final String toString()
    {
        final StringBuilder buf = new StringBuilder();
        
        buf.append( this.name );
        buf.append( ": " );
        
        if( ! this.enabled )
        {
            buf.append( "disabled" );
        }
        else if( this.failed )
        {
            buf.append( "failed" );
        }
        else if( this.startTime == 0 )
        {
            buf.append( "not run" );
        }
        else
        {
            final BigDecimal totalTime = new BigDecimal( ( (double) ( this.endTime - this.startTime ) ) / ( 1000 * 60 ) ).setScale( 2, BigDecimal.ROUND_HALF_UP );
            
            buf.append( recordsPerSecond() );
            buf.append( "/sec (" );
            buf.append( totalSize() );
            buf.append( " records in " );
            buf.append( totalTime );
            buf.append( " min)" );
        }
        
        return buf.toString();
    }
    
}
