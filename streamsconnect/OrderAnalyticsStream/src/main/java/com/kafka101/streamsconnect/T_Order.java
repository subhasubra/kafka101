package com.kafka101.streamsconnect;

import lombok.Data;

@Data
public class T_Order {
    private String orderId;
    private Integer amount;
    private String status;
    private String customerId;
}
