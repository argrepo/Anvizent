package com.prifender.des.mock.pump.relational;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.prifender.des.mock.pump.Config;
import com.prifender.des.mock.pump.relational.client.RelationalDatabaseClient;
import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;

@Component
public final class RelationalTablePumpProcess
{
    @Autowired
    private Config config;
    
    @Autowired
    protected RelationalDatabaseClient database;
    
    @Autowired
    public MessagingConnectionFactory messagingConnectionFactory;
    
    public void run( final String tableName, final String mockDesOutputQueueName ) throws Exception
    {
        final Table table = Schema.table( tableName );
        final String collection = table.collection();
        final List<String> attributes = table.attributes();
        
        try (final Connection databaseConnection = this.database.connect()) {
            
            final StringBuilder insertQuery = new StringBuilder();
            
            // System.out.println( "Using insert query..." );
            
            insertQuery.append( "INSERT INTO " ).append( this.database.namespace() ).append( '.' ).append( collection ).append( " VALUES ( " );
            
            for( int i = 0, n = attributes.size(); i < n; i++ )
            {
                if( i > 0 )
                {
                    insertQuery.append( ", " );
                }
                
                insertQuery.append( '?' );
            }
            
            insertQuery.append( " );" );
            
            final String insertQueryStr = insertQuery.toString();
            
            // System.out.println( insertQueryStr );

            try (final PreparedStatement st = databaseConnection.prepareStatement(insertQueryStr)) {
                try (final MessagingConnection connection = this.messagingConnectionFactory.connect())
                {
                    // System.out.println( "Transfering mock data from queue " + mockDesOutputQueueName );
                    
                    final MessagingQueue queue = connection.queue(mockDesOutputQueueName);

                    queue.consume(new MessageConsumer() {

                        private int queryCountInBuffer = 0;
                        private int insertedRecordsCount = 0;
                        private long timeStarted = System.currentTimeMillis();

                        @Override
                        public void consume(final byte[] message) {
                            final String msg;
                            try {
                                msg = new String(message, "UTF-8");
                            } catch (final UnsupportedEncodingException e) {
                                e.printStackTrace();
                                return;
                            }

                            final JsonParser parser = new JsonParser();
                            final JsonObject json = (JsonObject) parser.parse(msg);

                            try {
                                st.clearParameters();
                                table.copyDataIntoPreparedStatement(json, st);
                                st.addBatch();

                                this.queryCountInBuffer++;

                                if (this.queryCountInBuffer == config.perfBatchSize) {
                                    st.executeBatch();

                                    this.queryCountInBuffer = 0;

                                    this.insertedRecordsCount += config.perfBatchSize;

                                    final int recordsPerSecond = (int) (((double) this.insertedRecordsCount / (System.currentTimeMillis() - this.timeStarted)) * 1000);
                                    System.out.println(recordsPerSecond + "/sec");
                                }
                            } catch (final Exception e) {
                                System.out.println(e);
                            }
                        }
                    }, 30 * 1000 );
                    
                    // Execute the final batch and exit.
                    
                    try
                    {
                        st.executeBatch();
                    }
                    catch( final Exception e )
                    {
                        System.out.println( e );
                    }
                    
                    // System.out.println( "Have not seen messages in the queue for 30 seconds. Assuming the extraction job is complete." );
                }
            }
        }
    }

}
