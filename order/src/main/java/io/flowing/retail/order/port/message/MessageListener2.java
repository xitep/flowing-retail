package io.flowing.retail.order.port.message;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowing.retail.order.port.persistence.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.spin.plugin.variable.SpinValues;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;

@Component
public class MessageListener2 {

    @Autowired
    private OrderRepository repository;

    @Autowired
    private ProcessEngine camunda;

    @Autowired
    private ApplicationContext context;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "foobar");
        props.setProperty("enable.auto.commit", "false");
        Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "quux");
            t.setDaemon(true);
            return t;
        }).submit(() -> {
            ByteArrayDeserializer deser = new ByteArrayDeserializer();
            KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props, deser, deser);
            try {
                c.subscribe(Collections.singletonList("flowing-retail"));
                ObjectMapper om = new ObjectMapper();
                MessageListener2 bean = context.getBean(MessageListener2.this.getClass());
                while (true) {
                    ConsumerRecords<byte[], byte[]> msgs = c.poll(1_000);
                    if (!msgs.isEmpty()) {
                        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
                            String s = new String(msg.value(), 36, msg.value().length - 36, StandardCharsets.UTF_8);
                            Message<JsonNode> message = om.readValue(s, new TypeReference<Message<JsonNode>>() {});
                            if (message.getMessageType().endsWith("Event")) {
                                bean.messageReceived(message);
                                c.commitSync();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally{
                c.close();
            }
        });
    }

    /**
     * Very generic listener for simplicity. It takes all events and checks, if a
     * flow instance is interested. If yes, they are correlated,
     * otherwise they are just discarded.
     *
     * It might make more sense to handle each and every message type individually.
     */
//    @StreamListener(value = "input",
//            condition="payload.messageType.toString().endsWith('Event')")
    @Transactional
    public void messageReceived(Message<JsonNode> message) throws Exception {
//        System.out.println("Inspecting: " + message);
//        Message<JsonNode> message = new ObjectMapper().readValue( //
//                messageJson, //
//                new TypeReference<Message<JsonNode>>() {});

        long correlatingInstances = camunda.getRuntimeService().createExecutionQuery() //
                .messageEventSubscriptionName(message.getMessageType()) //
                .processInstanceBusinessKey(message.getTraceId()) //
                .count();

        if (correlatingInstances==1) {
//            System.out.println("Correlating " + message + " to waiting flow instance");

            camunda.getRuntimeService().createMessageCorrelation(message.getMessageType())
                    .processInstanceBusinessKey(message.getTraceId())
                    .setVariable(//
                            "PAYLOAD_" + message.getMessageType(), //
                            SpinValues.jsonValue(message.getPayload().toString()).create())//
                    .correlateWithResult();
        } else {
            // ignoring event, not interested
//            System.out.println("2: Order context ignores event '" + message.getMessageType() + "'");
        }

    }

}
