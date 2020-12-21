package com.kafka101.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.producer.event.OrderEvent;
import com.kafka101.producer.event.OrderEventProducer;
import com.kafka101.producer.event.OrderEventType;
import com.kafka101.producer.model.Customer;
import com.kafka101.producer.model.Order_T;
import com.kafka101.producer.model.OrderStatus;
import com.kafka101.producer.model.Product;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OrderEventProducerUnitTest {

    private final int ID_GEN_BOUND = Integer.MAX_VALUE;
    private final int PRICE_GEN_BOUND = 10000;
    private final String topic = "order-events-topic";
    Random randGen = new Random();
    Customer testCustomer;
    Product testProduct1;
    Product testProduct2;
    Order_T testOrder;

    @Mock
    KafkaTemplate kafkaTemplate;

    @Spy
    ObjectMapper objectMapper;

    @InjectMocks
    OrderEventProducer orderEventProducer;

    @BeforeEach
    void setUp() {

        // Create Test Customer
        testCustomer = Customer.builder()
                .customerId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
                .isPrime(true)
                .build();

        // Create Test Products
        testProduct1 = Product.builder()
                .productId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        testProduct2 = Product.builder()
                .productId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();

        // Create Test Order
        testOrder = Order_T.builder()
                .orderId(randGen.nextInt(ID_GEN_BOUND))
                .status(OrderStatus.NEW)
                .products(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customer(testCustomer)
                .build();
    }

    @Test
    public void sendOrderEvent_TestFailure() {
        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                .orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventType(OrderEventType.NEW)
                //.order(testOrder)
                .order(null)
                .build();

        // Mock Kafka Template and make it return a failure/exceptions
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception while sending message via Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // Call the Event Producer to publish message
        assertThrows(Exception.class, ()->orderEventProducer.sendOrderEvent(testOrderEvent).get());
    }

    @Test
    public void sendOrderEvent_Success() throws JsonProcessingException, ExecutionException, InterruptedException {

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                .orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();

        // Mock Kafka Template and make it return a success
        String message = objectMapper.writeValueAsString(testOrderEvent);
        SettableListenableFuture future = new SettableListenableFuture();
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord(topic, testOrderEvent.getOrderEventId(), message);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(topic, 1),
                1, 1, 342, System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> result = new SendResult<>(producerRecord, recordMetadata);
        future.set(result);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // Call the Event Producer to publish message
        assert orderEventProducer.sendOrderEvent(testOrderEvent).get().getRecordMetadata().partition() == 1;
    }
}
