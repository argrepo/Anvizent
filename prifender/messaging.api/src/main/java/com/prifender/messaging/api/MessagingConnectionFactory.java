package com.prifender.messaging.api;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Represents a connection to a messaging service, which could be a cloud of connected processes.
 */

@Component( "messagingConnectionFactory" )
public final class MessagingConnectionFactory
{
    @Autowired
    private MessagingService messagingService;
    
    private final String messagingServiceUri = readFromEnv( "MESSAGING_SERVICE", "amqp://localhost" );
    
    /**
     * Establishes a connection to the messaging service, using the default connection URI and the default adapter.
     * 
     * @return the connection to the messaging service
     * @throws IOException if failed to connect
     */
    
    public MessagingConnection connect() throws IOException
    {
        return this.messagingService.connect( this.messagingServiceUri );
    }
    
    private static final String readFromEnv( final String var, final String defaultValue )
    {
        if( var == null )
        {
            throw new IllegalArgumentException();
        }
        
        if( defaultValue == null )
        {
            throw new IllegalArgumentException();
        }
        
        String value = System.getenv( var );
        
        if( value == null || value.length() == 0 )
        {
            System.out.println( "Environment variable " + var + " was not set, so using " + defaultValue );
            value = defaultValue;
        }
        
        return value;
    }
    
}
