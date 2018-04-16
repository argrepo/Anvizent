package com.prifender.des.messaging.rabbitmq;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingQueue;
import com.prifender.messaging.rabbitmq.RabbitMessagingService;

public final class RabbitAdapterTest
{
    // URL for connecting to RabbitMQ in format amqp://user:password@host:port, for example amqp://localhost
    
    private static final String PROP_RABBIT = "RabbitAdapterTest.RabbitMQ";
    
    // Maximum number of milliseconds that producers and consumers should sleep between handling of messages
    // to simulate work delays. The actual sleep time will be a random value from 0 to max. Default max sleep
    // is 5000ms.
    
    private static final String PROP_MAX_SLEEP = "RabbitAdapterTest.MaxSleep";
    
    private static int getMaxSleep()
    {
        final String maxSleepStr = System.getProperty( PROP_MAX_SLEEP );
        
        if( maxSleepStr != null )
        {
            try
            {
                return Integer.parseInt( maxSleepStr );
            }
            catch( final NumberFormatException e ) {}
        }
        
        return 5000;
    }
    
    /**
     * Tests RabbitMQ adapter by using ten producer threads and five consumer threads to move numbers from 0 to 99
     * through a queue.
     */
    
    @Test
    public void testMultipleProducersAndConsumers() throws Exception
    {
        final String rabbit = System.getProperty( PROP_RABBIT );
        
        if( rabbit != null )
        {
            final String queueName = "RabbitAdapterTest-" + UUID.randomUUID().toString();
            final int maxSleep = getMaxSleep();
            final List<Thread> threads = new ArrayList<Thread>();
            
            System.out.println( "Using queue " + queueName );
            
            for( int i = 0; i < 10; i++ )
            {
                final int fi = i;
                
                final Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        final RabbitMessagingService service = new RabbitMessagingService();
                        
                        try( final MessagingConnection connection = service.connect( rabbit ) )
                        {
                            final MessagingQueue queue = connection.queue( queueName );
                            
                            for( int j = 0; j < 10; j++ )
                            {
                                if( maxSleep > 0 )
                                {
                                    final int sleep = (int) ( Math.random() * maxSleep );
                                    
                                    try 
                                    {
                                        Thread.sleep( sleep );
                                    }
                                    catch( final InterruptedException e )
                                    {
                                        e.printStackTrace();
                                    }
                                }
                                
                                final String message = String.valueOf( fi * 10 + j );
                                System.out.println( "Producer " + fi + " posting " + message );
                                queue.post( message );
                            }
                        }
                        catch( final IOException e )
                        {
                            e.printStackTrace();
                        }
                    }
                };
                
                threads.add( thread );
                thread.start();
            }
            
            final boolean[] results = new boolean[ 100 ];
            
            for( int i = 0; i < 5; i++ )
            {
                final int fi = i;
                
                final Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        final RabbitMessagingService service = new RabbitMessagingService();
                        
                        try( final MessagingConnection connection = service.connect( rabbit ) )
                        {
                            final MessagingQueue queue = connection.queue( queueName );
                            
                            queue.consume
                            (
                                new MessageConsumer()
                                {
                                    @Override
                                    public void consume( final byte[] message )
                                    {
                                        final String msg;
                                        
                                        try
                                        {
                                            msg = new String( message, "UTF-8" );
                                        }
                                        catch( final UnsupportedEncodingException e )
                                        {
                                            e.printStackTrace();
                                            return;
                                        }
                                        
                                        System.out.println( "Consumer " + fi + " received " + msg );
                                        
                                        synchronized( results )
                                        {
                                            final int number = Integer.parseInt( msg );
                                            
                                            if( results[ number ] )
                                            {
                                                System.out.println( "Message " + number + " received more than once" );
                                            }
                                            else
                                            {
                                                results[ number ] = true;
                                            }
                                        }
                                        
                                        if( maxSleep > 0 )
                                        {
                                            final int sleep = (int) ( Math.random() * maxSleep );
                                            
                                            try 
                                            {
                                                Thread.sleep( sleep );
                                            }
                                            catch( final InterruptedException e )
                                            {
                                                e.printStackTrace();
                                            }
                                        }
                                    }
                                },
                                10 * 1000
                            );
                        }
                        catch( final IOException e )
                        {
                            e.printStackTrace();
                        }
                    }
                };
                
                threads.add( thread );
                thread.start();
            }
            
            for( final Thread thread : threads )
            {
                thread.join();
            }
            
            for( int i = 0; i < 100; i++ )
            {
                if( ! results[ i ] )
                {
                    fail( "Did not received message " + i );
                }
            }
            
            final RabbitMessagingService service = new RabbitMessagingService();
            
            try( final MessagingConnection connection = service.connect( rabbit ) )
            {
                connection.queue( queueName ).delete();
            }
        }
    }

}
