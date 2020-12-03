package com.kafka101.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.producer.event.OrderEvent;
import com.kafka101.producer.event.OrderEventType;
import com.kafka101.producer.model.Customer;
import com.kafka101.producer.model.Order_T;
import com.kafka101.producer.model.OrderStatus;
import com.kafka101.producer.model.Product;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"order-events-topic"}, partitions = 3) // Using Embedded Kafka for tests
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class OrderControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    private final int ID_GEN_BOUND = Integer.MAX_VALUE;
    private final int PRICE_GEN_BOUND = 10000;
    private final String topic = "order-events-topic";
    Random randGen = new Random();
    Customer testCustomer;
    Product testProduct1;
    Product testProduct2;
    Order_T testOrder;

    private Consumer<Integer, String> testConsumer;


    @Autowired
    EmbeddedKafkaBroker testBroker;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerConfig = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", testBroker));
        testConsumer = new DefaultKafkaConsumerFactory<>(consumerConfig, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        testBroker.consumeFromAnEmbeddedTopic(testConsumer, topic);

        // Create Test Customer
        testCustomer = Customer.builder()
                .customerId(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
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

    @AfterEach
    void tearDown() {
        testConsumer.close();
    }

    @Test
    @Timeout(5)
    void postOrderEvent() throws JsonProcessingException {

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();

        // Send the request
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<OrderEvent> request = new HttpEntity<>(testOrderEvent, headers);
        ResponseEntity<OrderEvent> response = restTemplate.exchange("/v1/create_order",
                HttpMethod.POST,
                request,
                OrderEvent.class);

        // Check if the response is as expected
        Assertions.assertEquals(HttpStatus.CREATED, response.getStatusCode());

        // Consume the event using the test consumer
        ConsumerRecord message = KafkaTestUtils.getSingleRecord(testConsumer, topic);
        String expectedResult = objectMapper.writeValueAsString(response.getBody());

        String actualResult = message.value().toString();
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    @Timeout(5)
    void putOrderEvent() throws JsonProcessingException {

        testOrder.setStatus(OrderStatus.CANCELLED);

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                .orderEventId(randGen.nextInt(ID_GEN_BOUND))
                //.orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();

        // Send the request
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<OrderEvent> request = new HttpEntity<>(testOrderEvent, headers);
        ResponseEntity<OrderEvent> response = restTemplate.exchange("/v1/cancel_order",
                HttpMethod.PUT,
                request,
                OrderEvent.class);

        // Check if the response is as expected
        Assertions.assertEquals(HttpStatus.OK, response.getStatusCode());

        // Consume the event using the test consumer
        ConsumerRecord message = KafkaTestUtils.getSingleRecord(testConsumer, topic);
        String expectedResult = objectMapper.writeValueAsString(response.getBody());

        String actualResult = message.value().toString();
        Assertions.assertEquals(expectedResult, actualResult);
    }
}

