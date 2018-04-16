package com.prifender.messaging.api;

import java.io.IOException;

/**
 * Represents a single queue in a messaging service.
 */

public interface MessagingQueue
{
    /**
     * Posts a message to the queue.
     * 
     * @param message the message content
     * @throws IOException if failed while posting to the queue
     */
    
    void post( byte[] message ) throws IOException;
    
    /**
     * Posts a message to the queue.
     * 
     * @param message the message content
     * @throws IOException if failed while posting to the queue
     */
    
    default void post( final String message ) throws IOException
    {
        post( message.getBytes( "UTF-8" ) );
    }
    
    /**
     * Starts consuming messages from the queue. The messages are delivered to the supplied consumer object. This method
     * blocks and consumes messages indefinitely.
     * 
     * @param consumer the consumer object that will receive the messages
     * @throws IOException if something went wrong
     */
    
    default void consume( final MessageConsumer consumer ) throws IOException
    {
        consume( consumer, -1 );
    }
    
    /**
     * Starts consuming messages from the queue. The messages are delivered to the supplied consumer object. This method
     * blocks and consumes messages until message wait time exceeds the timeout.
     * 
     * @param consumer the consumer object that will receive the messages
     * @param timeout the number of milliseconds to wait for a message or -1 to wait indefinitely
     * @throws IOException if something went wrong
     */
    
    void consume( MessageConsumer consumer, long timeout ) throws IOException;
    
    /**
     * Deletes the queue.
     * 
     * @throws IOException if something went wrong
     */
    
    void delete() throws IOException;
    
}
