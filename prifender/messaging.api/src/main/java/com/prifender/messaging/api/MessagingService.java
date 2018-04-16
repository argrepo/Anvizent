package com.prifender.messaging.api;

import java.io.IOException;

/**
 * Represents a messaging service, which could be a cloud of connected processes.
 */

public interface MessagingService
{
    /**
     * Establishes a connection to the messaging service.
     * 
     * @param uri the service URI
     * @return the connection to the messaging service
     * @throws IOException if failed to connect
     */
    
    MessagingConnection connect( String uri ) throws IOException;
    
}
