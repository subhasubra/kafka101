package com.kafka101.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.consumer.model.*;
import com.kafka101.consumer.receiver.OrderEventsConsumer;
import com.kafka101.consumer.repository.CustomerRepository;
import com.kafka101.consumer.repository.OrderEventRepository;
import com.kafka101.consumer.repository.Order_T_Repository;
import com.kafka101.consumer.repository.ProductRepository;
import com.kafka101.consumer.service.OrderEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.replica.RackAwareReplicaSelector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//assignment.consumer.rack=in-east-1
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"order-events-topic"}, partitions = 3, brokerProperties = {"broker.rack=in-east-1"}) // Using Embedded Kafka for tests
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.properties.partition.assignment.strategy=com.kafka101.consumer.assignor.RackAwareAssignor",
        "spring.kafka.consumer.client-rack=in-east-1",
        "assignment.consumer.rack=in-east-1"})
public class OrderEventsConsumerIntegrationTest {

    private final int ID_GEN_BOUND = Integer.MAX_VALUE;
    private final int PRICE_GEN_BOUND = 10000;
    private String topic = "order-events-topic";
    private Customer testCustomer;
    private Product testProduct1;
    private Product testProduct2;
    private Order_T testOrder;
    // Random number generators for ID
    Random randGen = new Random();

    @Autowired
    EmbeddedKafkaBroker testBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    OrderEventsConsumer orderEventsConsumer;

    @SpyBean
    OrderEventService orderEventService;

    @Autowired
    OrderEventRepository orderEventRepository;

    @Autowired
    Order_T_Repository orderRepository;

    @Autowired
    CustomerRepository customerRepository;

    @Autowired
    ProductRepository productRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, testBroker.getPartitionsPerTopic());
        }

        // Create & Persist Test Customer
        testCustomer = Customer.builder()
                .customerId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
                .isPrime(false)
                .build();
        customerRepository.save(testCustomer);

        // Create & Persist Test Products
        testProduct1 = Product.builder()
                .productId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        productRepository.save(testProduct1);
        testProduct2 = Product.builder()
                .productId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        productRepository.save(testProduct2);

        // Create Test Order
        testOrder = Order_T.builder()
                .orderId(randGen.nextInt(ID_GEN_BOUND))
                .status(OrderStatus.NEW)
                .products(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customer(testCustomer)
                .build();
    }

    @AfterEach
    void tearDown() {
        // Remove the data from H2 DB
        customerRepository.deleteAll();
        productRepository.deleteAll();
    }

    @Test
    void publishOrderEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();
        kafkaTemplate.send(topic, objectMapper.writeValueAsString(testOrderEvent)).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(orderEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(orderEventService, times(1)).processOrderEvent(isA(ConsumerRecord.class));

        List<OrderEvent> orderEventList = (List<OrderEvent>) orderEventRepository.findAll();
        assert orderEventList.size() == 1;
        orderEventList.forEach(orderEvent -> {
            assert orderEvent.getOrderEventId() != null;
            Assertions.assertEquals(testOrder.getOrderId(), orderEvent.getOrder().getOrderId());
        });
    }

    @Test
    void updateOrderEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        // Update the order
        orderRepository.save(testOrder);
        Float tpUpdated = 150.75F;
        testOrder.setTotalPrice(tpUpdated);
        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.UPDATE)
                .order(testOrder)
                .build();
        kafkaTemplate.send(topic, objectMapper.writeValueAsString(testOrderEvent)).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(orderEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(orderEventService, times(1)).processOrderEvent(isA(ConsumerRecord.class));

        List<OrderEvent> orderEventList = (List<OrderEvent>) orderEventRepository.findAll();
        assert orderEventList.size() == 1;
        orderEventList.forEach(orderEvent -> {
            assert orderEvent.getOrderEventId() != null;
            Assertions.assertEquals(tpUpdated, orderEvent.getOrder().getTotalPrice());
        });
    }

    @Test
    void updateOrderEvent_Failure() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        // Update the order
        orderRepository.save(testOrder);
        Float tpUpdated = 150.75F;
        testOrder.setTotalPrice(tpUpdated);
        testOrder.setOrderId(null);
        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.UPDATE)
                .order(testOrder)
                .build();
        kafkaTemplate.send(topic, objectMapper.writeValueAsString(testOrderEvent)).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(orderEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(orderEventService, times(1)).processOrderEvent(isA(ConsumerRecord.class));
    }

    @Test
    void updateOrderEvent_RecoverableFailure() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        // Update the order
        orderRepository.save(testOrder);
        testOrder.setOrderId(0);
        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.UPDATE)
                .order(testOrder)
                .build();
        kafkaTemplate.send(topic, objectMapper.writeValueAsString(testOrderEvent)).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(orderEventsConsumer, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(orderEventService, times(3)).processOrderEvent(isA(ConsumerRecord.class));
    }

}