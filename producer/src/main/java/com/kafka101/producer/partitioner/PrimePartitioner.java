package com.kafka101.producer.partitioner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka101.producer.event.OrderEvent;
import com.kafka101.producer.model.Order_T;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Assigns Partitions based on Customer Type (Prime Customers will be assigned a special partition)
 */
@Slf4j
public class PrimePartitioner implements Partitioner {

    @Autowired
    DefaultPartitioner defaultPartitioner;

    @SneakyThrows
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        int partitionValue = 0;

        // Get the number of partitions for this topic
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // Check for Key Datatype. In our use case, null key is permitted, so don't throw.
        if ((keyBytes == null) || (!(key instanceof Integer))) {
            log.info("Key is null......");
            //throw new InvalidRecordException("Keys should be of type Integer");
        }


        // If Customer is a prime customer then assign a separate partition
        ObjectMapper objMapper = new ObjectMapper();
        Order_T order = objMapper.readValue(valueBytes, OrderEvent.class).getOrder();
        if (order != null) {
            if (order.getCustomer().isPrime()) {
                log.info("Order from Prime Customer. Routing it to special partition....");
                partitionValue = numPartitions -1;
            } else {
                // Other records will get hashed to rest of the partitions
                log.info("Order from Regular Customer. Routing it to regular partition(s)....");
                partitionValue =  Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 2);
            }
        } else {
            // send the default partition, otherwise
            partitionValue = defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
        log.info("******** Custom Partitioner: PrimePartitioner routing message to partition:" + partitionValue);
        return partitionValue;
    }

    @Override
    public void close() {
        // Nothing much to do
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Nothing much to do
    }
}
