package com.kafka101.consumer.repository;

import com.kafka101.consumer.model.OrderEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderEventRepository extends CrudRepository<OrderEvent, Integer> {
}
