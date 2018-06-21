package com.prifender.messaging.rabbitmq;

import java.io.IOException;

import com.prifender.messaging.api.MessageConsumer;
import com.prifender.messaging.api.MessagingQueue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;

public final class RabbitQueue implements MessagingQueue
{
    private final Channel channel;
    private final String name;
    
    RabbitQueue( final Channel channel, final String name ) throws IOException
    {
        if( channel == null )
        {
            throw new IllegalArgumentException();
        }
        
        if( name == null )
        {
            throw new IllegalArgumentException();
        }
        
        this.channel = channel;
        this.name = name;
        
        declareQueue();
    }
    
    @Override
    public void post( final byte[] message ) throws IOException
    {
        if( message == null )
        {
            throw new IllegalArgumentException();
        }
        
        declareQueue();
        
        this.channel.basicPublish( "", this.name, MessageProperties.PERSISTENT_BASIC, message );
    }
    
    @Override
    public void consume( final MessageConsumer consumer, final long timeout ) throws IOException
    {
        if( consumer == null )
        {
            throw new IllegalArgumentException();
        }
        
        declareQueue();
        
        long wait = 0;
        
        while( timeout < 0 || wait < timeout )
        {
            final GetResponse response = this.channel.basicGet( this.name, /* autoAck */ false );
            
            if( response == null )
            {
                try
                {
                    Thread.sleep( 1000 );
                    wait += 1000;
                }
                catch( final InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                wait = 0;
                
                try
                {
                    consumer.consume( response.getBody() );
                    this.channel.basicAck( response.getEnvelope().getDeliveryTag(), /* multiple */ false );
                }
                catch( final Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    @Override
    public void delete() throws IOException
    {
        this.channel.queueDelete( this.name );
    }
    
    private void declareQueue() throws IOException
    {
        this.channel.queueDeclare( this.name, /* durable */ true, /* exclusive */ false, /* autoDelete */ false, /* arguments */ null );
    }

}
