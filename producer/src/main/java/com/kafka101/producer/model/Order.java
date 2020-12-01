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
public class Order {
    @NotNull(message = "Order ID cannot be null")
    private Integer id;

    @NotNull
    private List<Product> productsList;

    @NotNull
    private Float totalPrice;

    @NotNull
    private OrderStatus status;

    @NotNull
    private Customer customerDetails;
}