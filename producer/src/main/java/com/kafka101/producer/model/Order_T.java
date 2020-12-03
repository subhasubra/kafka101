package com.kafka101.producer.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.Builder;

import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Order_T {
    @NotNull(message = "Order ID cannot be null")
    private Integer orderId;

    @NotNull
    private List<Product> products;

    @NotNull
    private Float totalPrice;

    @NotNull
    private OrderStatus status;

    @NotNull
    private Customer customer;
}