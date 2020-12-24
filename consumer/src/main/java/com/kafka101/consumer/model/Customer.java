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
    @Column(name="customer_id")
    private Integer customerId;

    @Column(name="name")
    private String name;

    @Column(name="email")
    private String email;

    @Column(name="is_prime")
    private boolean isPrime;

    @OneToMany(mappedBy = "customer", cascade = {CascadeType.ALL})
    private List<Order_T> orders;
}
