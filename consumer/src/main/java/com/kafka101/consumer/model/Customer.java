package com.kafka101.consumer.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.Builder;

import javax.persistence.*;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Customer {
    @Id
    private Integer customerId;

    private String name;

    private String email;

    @OneToMany(mappedBy = "customer", cascade = {CascadeType.ALL})
    private List<Order_T> orders;
}
