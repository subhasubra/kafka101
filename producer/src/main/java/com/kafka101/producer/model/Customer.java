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
public class Customer {
    @NotNull
    private Integer id;

    @NotBlank
    private String name;

    @NotBlank
    private String email;
}