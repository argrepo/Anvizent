package com.prifender.des.mock.pump.relational;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import com.prifender.des.client.DefaultApi;
import com.prifender.des.client.model.DataExtractionAttribute;
import com.prifender.des.client.model.DataExtractionJob;
import com.prifender.des.client.model.DataExtractionSpec;
import com.prifender.des.client.model.DataExtractionSpec.ScopeEnum;
import com.prifender.des.client.model.DataSource;
import com.prifender.des.mock.pump.Config;
import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;

public final class RelationalTablePump
{
    private final Config config;
    private final RelationalDatabaseClient database;
    private final DefaultApi mockDes;
    private final DataSource ds;
    private final Table table;
    
    public RelationalTablePump( final Config config, final RelationalDatabaseClient database, final DefaultApi mockDes, final DataSource ds, final Table table )
    {
        if( config == null )
        {
            throw new IllegalArgumentException();
        }

        this.config = config;
        
        if( database == null )
        {
            throw new IllegalArgumentException();
        }

        this.database = database;
        
        if( mockDes == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.mockDes = mockDes;
        
        if( ds == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.ds = ds;
        
        if( table == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.table = table;
    }
    
    public final void pump() throws Exception
    {
        final String collection = this.table.collection();
        final List<String> attributes = this.table.attributes();
        
        System.out.println( "Starting data extraction for collection " + collection + "..." );
        
        final DataExtractionSpec spec = new DataExtractionSpec()
            .dataSource( this.ds.getId() )
            .collection( collection )
            .scope( ScopeEnum.ALL );
        
        for( final String attribute : attributes )
        {
            spec.addAttributesItem( new DataExtractionAttribute().name( attribute ) );
        }
                
        DataExtractionJob job = this.mockDes.startDataExtractionJob( spec );
        
        String queueName = job.getOutputMessagingQueue();
        
        while( queueName == null )
        {
            try
            {
                Thread.sleep( 1000 );
            }
            catch( final InterruptedException e ) {}
            
            job = this.mockDes.findDataExtractionJobById( job.getId() );
            queueName = job.getOutputMessagingQueue();
        }
        
        try (final Connection databaseConnection = this.database.connect()) {
            
            System.out.println( "Creating target table..." );
            
            final String targetTableDdl = this.table.createTableDdl( this.database.namespace() );
            System.out.println( targetTableDdl );
            
            try (final Statement st = databaseConnection.createStatement())
            {
                st.execute( targetTableDdl );
            }
        }
            
        final int perfParallelProcesses = this.config.perfParallelProcesses;
        final Process[] processes = new Process[ perfParallelProcesses ];
        final String cp = findCurrentClasspath();
        
        for( int i = 0; i < perfParallelProcesses; i++ )
        {
            final ProcessBuilder pb = new ProcessBuilder
            (
                "java",
                "-Ddatabase.url=" + String.valueOf( this.database.url() ),
                "-Ddatabase.namespace=" + String.valueOf( this.database.namespace() ),
                "-Dperf.batch.size=" + String.valueOf( this.config.perfBatchSize ),
                "-jar",
                cp,
                "-t",
                collection,
                queueName
            );
            
            final Process p = pb.start();
            
            processes[ i ] = p;
            
            new ChildOutputConsumerThread( i, p.getInputStream() ).start();
            new ChildOutputConsumerThread( i, p.getErrorStream() ).start();
        }
        
        for( int i = 0; i < perfParallelProcesses; i++ )
        {
            processes[ i ].waitFor();
        }
        
        try (final Connection databaseConnection = this.database.connect()) {
            
            try (final Statement st = databaseConnection.createStatement()) {
                final ResultSet rs = st.executeQuery("select count(*) from " + this.database.namespace() + "." + collection );
                
                rs.next();
                
                final int count = rs.getInt(1);
                final int expectedCount = this.table.collectionSizeMultiple() * this.config.mockDataSize;
                
                if( count == expectedCount )
                {
                    System.out.println( "Verified that the target table contains the expected number of rows (" + expectedCount + ")" );
                }
                else
                {
                    System.out.println( "Expecting target table to contain " + expectedCount + " rows, but found " + count + " rows" );
                    System.exit(1);
                }
            }
        }
    }
    
    public final void applyForeignKeys() throws Exception
    {
        final String foreignKeysDdl = this.table.foreignKeysDdl( this.database.namespace() );
        
        if( foreignKeysDdl != null )
        {
            try (final Connection databaseConnection = this.database.connect()) {
                
                System.out.println( foreignKeysDdl );
                
                try (final Statement st = databaseConnection.createStatement())
                {
                    st.execute( foreignKeysDdl );
                }
            }
        }
    }
    
    private static String findCurrentClasspath() throws Exception
    {
        final StringBuilder buf = new StringBuilder();
        
        for( final URL url: ((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs() )
        {
            if( buf.length() > 0 )
            {
                buf.append( File.pathSeparatorChar );
            }
            
            buf.append( new File( url.toURI() ).getAbsolutePath() );
        }
        
        return buf.toString();
    }
    
    private final class ChildOutputConsumerThread extends Thread
    {
        private final String linePrefix;
        private final BufferedReader reader;
        
        public ChildOutputConsumerThread( final int childProcessId, final InputStream in )
        {
            this.linePrefix = "[process-" + String.valueOf( childProcessId ) + "]: ";
            this.reader = new BufferedReader( new InputStreamReader( in ) );
        }
        
        @Override
        public void run()
        {
            try
            {
                for( String line = this.reader.readLine(); line != null; line = this.reader.readLine() )
                {
                    System.out.println( this.linePrefix + line );
                }
            }
            catch( final IOException e )
            {
                System.out.println( e );
            }
        }
    }

}
