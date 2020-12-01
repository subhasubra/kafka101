package com.kafka101.producer.event;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OrderEventProducer {
	
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	private ObjectMapper objectMapper; // for JSON style message
	
	private final String topic = "order-events-topic"; // add this to config - TBD
	
	// Async way of publishing an order event to the order-events-topic
	public ListenableFuture<SendResult<Integer, String>> sendOrderEvent(OrderEvent orderEvent) throws JsonProcessingException {
		
		Integer key = orderEvent.getOrderEventId();
		String value = objectMapper.writeValueAsString(orderEvent);
		
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value);
		ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(producerRecord);
		
		// Uncomment for a simple key/value based publish of messages w.o ProducerRecord
		//ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, key, value);

		// Using lambda
		/*future.addCallback(result -> {
			log.info("OrderEventProducer:Event published on topic : {} Successfully. Event Data-> key : {}, value : {}, result : {}",
					topic, key, value, result.getRecordMetadata().partition());
		}, ex -> {
			log.info("Event Publish Failed. Reason : {}", ex.getMessage());
		});*/

		future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				log.info("OrderEventProducer:Event published on topic : {} Successfully. Event Data-> key : {}, value : {}, result : {}", 
						topic, key, value, result.getRecordMetadata().partition());
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				log.info("Event Publish Failed. Reason : {}", ex.getMessage());
			}});

		return future;
		
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		// Meta Data related to the order. For example what was the source of the order and which marketing campaign it came from
		List<Header> metaData = List.of(new RecordHeader("source", "web".getBytes()),
				new RecordHeader("campaign", "none".getBytes()));
		return new ProducerRecord<Integer, String>(topic, null, key, value, metaData);
	}

	// Asynch way of publishing an order event to the prder-events-topic
	public SendResult<Integer, String> sendOrderEventSync(OrderEvent orderEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		
		Integer key = orderEvent.getOrderEventId();
		String value = objectMapper.writeValueAsString(orderEvent);
		
		SendResult<Integer, String> result = null;
		
		result = kafkaTemplate.send(topic, key, value).get(1, TimeUnit.SECONDS);
		log.info("OrderEventProducer: Event published on topic : {} Successfully. Event Data-> key : {}, value : {}, result : {}", 
				topic, key, value, result.getRecordMetadata().partition());
		return result;
		
	}

}
