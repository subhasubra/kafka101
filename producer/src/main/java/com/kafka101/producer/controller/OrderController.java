package com.kafka101.producer.controller;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.springframework.kafka.support.SendResult;

import com.kafka101.producer.event.OrderEvent;
import com.kafka101.producer.event.OrderEventProducer;
import com.kafka101.producer.event.OrderEventType;

import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;


@RestController
@Slf4j
public class OrderController {
	
	@Autowired
	OrderEventProducer orderEventProducer;
	
	//Async Create Order
	@PostMapping(value = "/v1/create_order")
	public ResponseEntity<OrderEvent> createOrder(@RequestBody @Valid OrderEvent orderEvent) throws JsonProcessingException {
		log.info("OrderController: Received Order : Processing Asynchronously {}", orderEvent.toString());
		
		// publish the event
		orderEvent.setOrderEventType(OrderEventType.NEW);
		orderEventProducer.sendOrderEvent(orderEvent);
		
		log.info("OrderController: Order Event Sent...");
		
		return ResponseEntity.status(HttpStatus.CREATED).body(orderEvent);
	}

	//Sync Create Order
	@PostMapping(value = "/v1/sync/create_order")
	public ResponseEntity<OrderEvent> createOrderSync(@Valid @RequestBody OrderEvent orderEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		log.info("OrderController: Received Order : Processing Synchronously {}", orderEvent.toString());
		
		// publish the event
		orderEvent.setOrderEventType(OrderEventType.NEW);
		SendResult<Integer, String> result = orderEventProducer.sendOrderEventSync(orderEvent);
		
		log.info("OrderController: Order Event Sent...");
		
		return ResponseEntity.status(HttpStatus.CREATED).body(orderEvent);
	}

	//Async Cancel Order
	@PutMapping(value = "/v1/cancel_order")
	public ResponseEntity<OrderEvent> cancelOrder(@RequestBody @Valid OrderEvent orderEvent) throws JsonProcessingException {
		log.info("OrderController: Received Cancel Order : Processing Request {}", orderEvent.toString());

		if (orderEvent.getOrderEventId() == null)
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(orderEvent);

		// publish the event
		orderEvent.setOrderEventType(OrderEventType.UPDATE);
		orderEventProducer.sendOrderEvent(orderEvent);
		
		log.info("OrderController: Cancel Order Event Sent...");
		
		return ResponseEntity.status(HttpStatus.OK).body(orderEvent);
	}
}
