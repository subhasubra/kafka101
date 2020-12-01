package com.kafka101.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.producer.event.OrderEvent;
import com.kafka101.producer.event.OrderEventProducer;
import com.kafka101.producer.event.OrderEventType;
import com.kafka101.producer.model.Customer;
import com.kafka101.producer.model.Order;
import com.kafka101.producer.model.OrderStatus;
import com.kafka101.producer.model.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest
@AutoConfigureMockMvc
public class OrderControllerUnitTest {

    private final int ID_GEN_BOUND = Integer.MAX_VALUE;
    private final int PRICE_GEN_BOUND = 10000;
    //private final String topic = "order-events-topic";

    @Autowired
    MockMvc mockMVC;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    OrderEventProducer orderEventProducer;

    @Test
    void postOrderEvent() throws Exception {

        // Random number generators for ID
        Random randGen = new Random();

        // Create Test Customer
        Customer testCustomer = Customer.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
                .build();

        // Create Test Products
        Product testProduct1 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        Product testProduct2 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();

        // Create Test Order
        Order testOrder = Order.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .status(OrderStatus.NEW)
                .productsList(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customerDetails(testCustomer)
                .build();

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();

        // Mock the Event Producer as the Controller is dependent on it.
        when(orderEventProducer.sendOrderEvent(testOrderEvent)).thenReturn(null);

        // perform the post operation
        mockMVC.perform(MockMvcRequestBuilders.post("/v1/create_order")
        .content(objectMapper.writeValueAsString(testOrderEvent))
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    }

    @Test
    void postOrderEvent_4xx() throws Exception {
        // Random number generators for ID
        Random randGen = new Random();

        // Create Test Customer
        Customer testCustomer = Customer.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                //.name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .name("")
                .email("janedoe@gmail.com")
                .build();

        // Create Test Products
        Product testProduct1 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        Product testProduct2 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();

        // Create Test Order
        Order testOrder = Order.builder()
                //.id(randGen.nextInt(ID_GEN_BOUND))
                .id(null)
                .status(OrderStatus.NEW)
                .productsList(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customerDetails(testCustomer)
                .build();

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                //.orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                //.order(null)
                .build();

        // Mock the Event Producer as the Controller is dependent on it.
        when(orderEventProducer.sendOrderEvent(testOrderEvent)).thenReturn(null);

        // perform the post operation
        mockMVC.perform(MockMvcRequestBuilders.post("/v1/create_order")
                .content(objectMapper.writeValueAsString(testOrderEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());
    }

    @Test
    void putOrderEvent() throws Exception {

        // Random number generators for ID
        Random randGen = new Random();

        // Create Test Customer
        Customer testCustomer = Customer.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
                .build();

        // Create Test Products
        Product testProduct1 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        Product testProduct2 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();

        // Create Test Order
        Order testOrder = Order.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .status(OrderStatus.NEW)
                .productsList(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customerDetails(testCustomer)
                .build();

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                .orderEventId(randGen.nextInt(ID_GEN_BOUND))
                .orderEventType(OrderEventType.UPDATE)
                .order(testOrder)
                .build();

        // Mock the Event Producer as the Controller is dependent on it.
        when(orderEventProducer.sendOrderEvent(testOrderEvent)).thenReturn(null);

        // perform the post operation
        mockMVC.perform(MockMvcRequestBuilders.put("/v1/cancel_order")
                .content(objectMapper.writeValueAsString(testOrderEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is2xxSuccessful());
    }

    @Test
    void putOrderEvent_4xx() throws Exception {
        // Random number generators for ID
        Random randGen = new Random();

        // Create Test Customer
        Customer testCustomer = Customer.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestCustomer" + randGen.nextInt(PRICE_GEN_BOUND))
                .email("janedoe@gmail.com")
                .build();

        // Create Test Products
        Product testProduct1 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();
        Product testProduct2 = Product.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .name("TestProduct" + randGen.nextInt(PRICE_GEN_BOUND))
                .price(Float.valueOf(randGen.nextInt(PRICE_GEN_BOUND)))
                .quantity(1)
                .build();

        // Create Test Order
        Order testOrder = Order.builder()
                .id(randGen.nextInt(ID_GEN_BOUND))
                .status(OrderStatus.NEW)
                .productsList(List.of(testProduct1, testProduct2))
                .totalPrice(testProduct1.getPrice() + testProduct2.getPrice())
                .customerDetails(testCustomer)
                .build();

        // Create Test OrderEvent
        OrderEvent testOrderEvent = OrderEvent.builder()
                .orderEventId(null)
                .orderEventType(OrderEventType.NEW)
                .order(testOrder)
                .build();

        // Mock the Event Producer as the Controller is dependent on it.
        when(orderEventProducer.sendOrderEvent(testOrderEvent)).thenReturn(null);

        // perform the post operation
        mockMVC.perform(MockMvcRequestBuilders.post("/v1/cancel_order")
                .content(objectMapper.writeValueAsString(testOrderEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());
    }
}
