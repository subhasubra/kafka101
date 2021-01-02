package com.kafka101.streamsconnect;

import lombok.Data;

@Data
public class OrderMetrics {
    private int orderCount;
    private int maxOrderAmount;

    public OrderMetrics update(T_Order order, OrderMetrics current) {
        if (current != null) {
            orderCount = current.getOrderCount() + 1;
            if (order.getAmount() > current.getMaxOrderAmount())
                maxOrderAmount = order.getAmount();
        } else {
            orderCount++;
            maxOrderAmount = order.getAmount();
        }
        return this;
    }
}
