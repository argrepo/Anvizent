package com.prifender.messaging.rabbitmq;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Component;

import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingService;
import com.rabbitmq.client.ConnectionFactory;

@Component( "messagingService" )
public final class RabbitMessagingService implements MessagingService
{
    @Override
    public MessagingConnection connect( final String uri ) throws IOException
    {
        try
        {
            final ConnectionFactory factory = new ConnectionFactory();
            factory.setUri( uri );
            
            return new RabbitConnection( factory.newConnection() );
        }
        catch( final TimeoutException | KeyManagementException | NoSuchAlgorithmException | URISyntaxException e )
        {
            throw new IOException( e.getMessage(), e );
        }
    }

}
