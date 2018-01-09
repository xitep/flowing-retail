package io.flowing.retail.order.port.message;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowing.retail.order.domain.Order;
import io.flowing.retail.order.port.persistence.OrderRepository;
import org.camunda.bpm.engine.ProcessEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;

@Component
@EnableBinding(Sink.class)
public class MessageListener {
  
  @Autowired
  private OrderRepository repository;
  
  @Autowired
  private ProcessEngine camunda;
    
  /**
   * Handles incoming OrderPlacedEvents. 
   * 
   *  Using the conditional {@link StreamListener} from 
   * https://github.com/spring-cloud/spring-cloud-stream/blob/master/spring-cloud-stream-core-docs/src/main/asciidoc/spring-cloud-stream-overview.adoc
   * in a way close to what Axion
   *  would do (see e.g. https://dturanski.wordpress.com/2017/03/26/spring-cloud-stream-for-event-driven-architectures/)
   */
  @StreamListener(target = Sink.INPUT, 
      condition="payload.messageType.toString()=='OrderPlacedEvent'")
  @Transactional
  public void orderPlacedReceived(String messageJson) throws JsonParseException, JsonMappingException, IOException, InterruptedException {
    Message<Order> message = new ObjectMapper().readValue(messageJson, new TypeReference<Message<Order>>(){});
    Order order = message.getPayload();

//    System.out.println("New order placed, start flow. " + order);
    
    // persist domain entity
    order.setOrderPlacedRecievedTs(System.currentTimeMillis());
    repository.createOrder(order);
    
    // and kick of a new flow instance
    camunda.getRuntimeService().createMessageCorrelation(message.getMessageType())
      .processInstanceBusinessKey(message.getTraceId())
      .setVariable("orderId", order.getId())
      .correlateWithResult();
  }

}
