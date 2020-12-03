package com.kafka101.consumer.model;

import lombok.*;

import javax.persistence.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class OrderEvent {
    @Id
    @GeneratedValue
    private Integer orderEventId;

    @OneToOne()
    @JoinColumn(name = "orderId")
    private Order_T order;

    @Enumerated(EnumType.STRING)
    private OrderEventType orderEventType;

}
