package com.prifender.messaging.api;

import java.io.IOException;

/**
 * Represents a connection to a messaging service.
 */

public interface MessagingConnection extends AutoCloseable
{
    /**
     * Returns a connection to the queue of the given name, creating a new queue if necessary
     * 
     * @param name the name of the queue
     * @return the connection to the queue
     * @throws IOException if failed while getting a connection to the queue
     */
    
    MessagingQueue queue( String name ) throws IOException;
    
    /**
     * Closes the connection and releases any associated resources.
     */
    
    void close();
    
}
