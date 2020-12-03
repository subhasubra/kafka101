package com.kafka101.consumer.repository;

import com.kafka101.consumer.model.Order_T;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Order_T_Repository extends CrudRepository<Order_T, Integer> {
}
