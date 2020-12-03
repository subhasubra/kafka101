package com.kafka101.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.consumer.model.OrderEvent;
import com.kafka101.consumer.model.Order_T;
import com.kafka101.consumer.repository.OrderEventRepository;
import com.kafka101.consumer.repository.Order_T_Repository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class OrderEventService {

    @Autowired
    OrderEventRepository orderEventRepository;

    @Autowired
    Order_T_Repository orderRepository;

    @Autowired
    ObjectMapper objectMapper;

    public void processOrderEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        OrderEvent orderEvent = objectMapper.readValue(consumerRecord.value(), OrderEvent.class);
        Order_T order = objectMapper.readValue(consumerRecord.value(), Order_T.class);
        log.info("OrderEventService:processOrderEvent Processing Order Event: {} and Order : {}", orderEvent, order);

        // Save or Update based on the Event Type
        switch(orderEvent.getOrderEventType()) {
            case NEW:
                save(orderEvent);
                break;
            case UPDATE:
                update(orderEvent);
                break;
            default:
                log.info("Invalid Order Event Type: {}", orderEvent.getOrderEventType());
                break;
        }
    }

    // Save Order and then the Order Event along with the Order FK
    private void save(OrderEvent orderEvent) {
        // First save the order
        log.info("Trying to Persist Order & Event : {}", orderEvent);
        orderRepository.save(orderEvent.getOrder());
        orderEventRepository.save(orderEvent);
        log.info("Successfully Persisted Order & Event : {}", orderEvent);
    }

    // Validate and update the Order
    private void update(OrderEvent orderEvent) {
        // check if the Order Id is Null/Valid
        Integer orderId = orderEvent.getOrder().getOrderId();
        if (orderId == null)
            throw new IllegalArgumentException("Order ID cannot be NULL");
        if(orderRepository.findById(orderId).isPresent() == false )
            throw new IllegalArgumentException("Invalid Order");

        // If Valid, Persist the changes
        orderRepository.save(orderEvent.getOrder());
        orderEventRepository.save(orderEvent);
        log.info("Successfully Persisted Order & Event : {}", orderEvent);
    }
}
