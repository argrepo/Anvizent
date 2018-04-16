package com.prifender.des.node;

import com.prifender.messaging.api.MessagingConnection;
import com.prifender.messaging.api.MessagingConnectionFactory;
import com.prifender.messaging.api.MessagingQueue;
import java.io.IOException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@ComponentScan({"com.prifender"})
@Configuration
@Component
public class GetMessagingQueue
{
  private ApplicationContext applicationContext;
  private MessagingConnectionFactory messaging;
  
  public GetMessagingQueue() {}
  
  public MessagingQueue getQueue(String queueName) throws IOException
  {
    applicationContext = new AnnotationConfigApplicationContext(new Class[] { GetMessagingQueue.class });
    messaging = ((MessagingConnectionFactory)applicationContext.getBean("messagingConnectionFactory"));
    MessagingConnection messagingServiceConnection = messaging.connect();
    return messagingServiceConnection.queue(queueName);
  }
}
