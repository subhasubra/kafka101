package com.kafka101.consumer.receiver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka101.consumer.service.OrderEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
//Uncomment interface implementation for Manual Ack
public class OrderEventsConsumer  /*implements AcknowledgingMessageListener*/ {

    @Autowired
    OrderEventService orderEventService;

    @KafkaListener(topics = {"order-events-topic"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Consumer Record : {}", consumerRecord);
        // process the message
        orderEventService.processOrderEvent(consumerRecord);
    }

    // Uncomment for Manual Ack
    /*
    @Override
    public void onMessage(ConsumerRecord data, Acknowledgment acknowledgment) {
        log.info("Consumer Record : {}", consumerRecord);
        acknowledgment.acknowledge();
    }
    */
}
