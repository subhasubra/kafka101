package com.kafka101.producer.event;

import com.kafka101.producer.model.Order_T;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.Builder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class OrderEvent {
	private Integer orderEventId;

	@NotNull(message = "Order cannot be null")
	@Valid
	private Order_T order;

	private OrderEventType orderEventType;

}
