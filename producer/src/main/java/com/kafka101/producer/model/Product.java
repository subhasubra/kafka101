package com.kafka101.producer.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.Builder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Product {
    @NotNull
    private Integer productId;

    @NotBlank
    private String name;

    @NotNull
    private Float price;

    @NotNull
    private Integer quantity;
}
