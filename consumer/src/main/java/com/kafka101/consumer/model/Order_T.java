package com.kafka101.consumer.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.Builder;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Order_T {

    @Id
    private Integer orderId;

    @ManyToMany
    @JoinTable(name = "order_products",
            joinColumns = @JoinColumn(name = "productId"),
            inverseJoinColumns = @JoinColumn(name = "orderId"))
    private List<Product> products;

    private Float totalPrice;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @ManyToOne
    @JoinColumn(name = "customerId")
    private Customer customer;

    @OneToOne(mappedBy = "order", cascade = {CascadeType.ALL})
    private OrderEvent orderEvent; // reverse foreign key reference

}
