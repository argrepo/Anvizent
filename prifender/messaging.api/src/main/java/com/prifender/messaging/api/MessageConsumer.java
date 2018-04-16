package com.prifender.messaging.api;

/**
 * Interface implemented by those wishing to consume messages.
 */

public interface MessageConsumer
{
    /**
     * Called by the messaging service, when a message is ready to be consumed.
     * 
     * @param message the message content
     */
    
    void consume( byte[] message );

}
