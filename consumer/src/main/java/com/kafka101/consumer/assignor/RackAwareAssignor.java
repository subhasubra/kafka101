package com.kafka101.consumer.assignor;

import com.kafka101.consumer.config.RackAwareAssignorConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.*;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Custom Partition Assignment Strategy based on Racks.
 * The Partitions are assigned to consumer which is part of the same rack
 * Setting Broker Rack Id: Use config parameter "broker.rack=rack-id"
 */
@Slf4j
public class RackAwareAssignor implements ConsumerPartitionAssignor, Configurable {

    private RackAwareAssignorConfig config;

    private final RangeAssignor defaultAssignor = new RangeAssignor();

    @SneakyThrows
    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        byte [] bytes = config.getRackId().getBytes(StandardCharsets.US_ASCII);
        return ByteBuffer.wrap(bytes);
    }

    @SneakyThrows
    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        log.info("********Partition Assignment Started..........");
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        Set<TopicPartition> allSubscribedTopics = new HashSet<>();
        Map<String, List<TopicPartition>> rawAssignments = new HashMap<>();
        Map <String, ArrayList<ConsumerRack>> consumerRackMap = new HashMap<>();
        Map<TopicPartition, String> leaderRackMap = new HashMap<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            // Get the consumer member and rack related info
            String memberId = subscriptionEntry.getKey();
            String consumerRack = StandardCharsets.US_ASCII.decode(subscriptionEntry.getValue().userData()).toString();
            if (memberId != null && consumerRack != null) {
                if (consumerRackMap.containsKey(consumerRack)) {
                    consumerRackMap.get(consumerRack).add(new ConsumerRack(memberId, consumerRack));
                } else{
                    ArrayList <ConsumerRack> al = new ArrayList<>();
                    al.add(new ConsumerRack(memberId, consumerRack));
                    consumerRackMap.put(consumerRack, al);
                }
                rawAssignments.put(memberId, new ArrayList<>());
            }

            // Populate the Topics and their Partitions
            List<String> topicList = subscriptionEntry.getValue().topics();
            for (String topic : topicList) {
                Integer numPartitions = metadata.partitionCountForTopic(topic);
                allSubscribedTopics.addAll(partitions(topic, numPartitions));
            }
        }

        // Populate the Topic-Partition Leader Rack Info
        for (TopicPartition topicPartition: allSubscribedTopics) {
            Node leaderNode = metadata.leaderFor(topicPartition);
            if (leaderNode != null && leaderNode.hasRack()) {
                leaderRackMap.put(topicPartition, leaderNode.rack());
            } else {
                leaderRackMap.put(topicPartition, "NO_RACK");
            }
        }

        boolean bItWorked = false;
        CircularIterator<ConsumerRack> assigner = null;
        // Assign Topic Partitions to Consumers based on Racks
        String prevRackId = null;
        for (Map.Entry<TopicPartition, String> leaderEntry : leaderRackMap.entrySet()) {
            TopicPartition partition = leaderEntry.getKey();
            String leaderRackId = leaderEntry.getValue();
            if (prevRackId == null || !prevRackId.equals(leaderRackId)) {
                if(consumerRackMap.containsKey(leaderRackId))
                    assigner = new CircularIterator<>(Utils.sorted(consumerRackMap.get(leaderRackId)));
                else
                    throw new KafkaException("Kafka Consumers/Brokers not configured properly for a Rack Aware setup. Please contact the Administrator");
            }
            while (!subscriptions.get(assigner.peek().memberId).topics().contains(partition.topic()))
                assigner.next();
            ConsumerRack consumer = assigner.next();
            log.info("********Consumer Rack ID:" + consumer.rackId);
            log.info("********Matching Consumer Found..........");
            rawAssignments.get(consumer.memberId).add(partition);
            bItWorked = true;
            prevRackId = leaderRackId;
        }

        // Fall back in case Racks are not present
        if (!bItWorked) {
            log.info("********Rack Id not found. Hence falling back to Round Robin Assignment");
            Set<String> topicsSubscribed = new HashSet<>();
            for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
                topicsSubscribed.addAll(subscriptionEntry.getValue().topics());

            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : topicsSubscribed) {
                Integer numPartitions = metadata.partitionCountForTopic(topic);
                if (numPartitions != null && numPartitions > 0)
                    partitionsPerTopic.put(topic, numPartitions);
                else
                    log.debug("Skipping assignment for topic {} since no metadata is available", topic);
            }
            rawAssignments.clear();
            rawAssignments = defaultAssignor.assign(partitionsPerTopic, subscriptions);
        }
        // Wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return new GroupAssignment(assignments);
    }

    private List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        defaultAssignor.onAssignment(assignment, metadata);
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return defaultAssignor.supportedProtocols();
    }

    @Override
    public short version() {
        return defaultAssignor.version();
    }

    @Override
    public String name() {
        return "RackAware";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new RackAwareAssignorConfig(configs);
    }

    private static class ConsumerRack implements Comparable<ConsumerRack> {

        private final String memberId;
        private final String rackId;

        ConsumerRack(final String memberId, final String rackId) {
            this.memberId = memberId;
            this.rackId = rackId;
        }

        @Override
        public int compareTo(final ConsumerRack other) {
            return this.rackId.compareTo(other.rackId);
        }
    }
}
