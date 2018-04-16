package com.prifender.messaging.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingQueue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public final class RabbitConnection implements MessagingConnection
{
    private final Connection connection;
    private final Channel channel;
    
    RabbitConnection( final Connection connection ) throws IOException
    {
        this.connection = connection;
        this.channel = connection.createChannel();
    }
    
    @Override
    public MessagingQueue queue( final String name ) throws IOException
    {
        return new RabbitQueue( this.channel, name );
    }

    @Override
    public void close()
    {
        try
        {
            try
            {
                this.channel.close();
            }
            finally
            {
                this.connection.close();
            }
        }
        catch( final IOException | TimeoutException e )
        {
            e.printStackTrace();
        }
    }

}
