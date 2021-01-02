package com.kafka101.consumer;

import com.kafka101.model.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Kafka Consumer that uses Avro Message Format and uses Confluent Schema Registry and Kafka provided Avro Deserializer
 */
public class KafkaAvroConfluentConsumer {
    private Properties m_Props;
    private final String m_Topic = "customerContacts";

    public void setProps(Properties props) {
        this.m_Props = props;
    }

    public void receiveMessage() throws InterruptedException {
        int wait = 5;
        System.out.println("Props=" + m_Props);
        KafkaConsumer<Integer, Customer> consumer = new KafkaConsumer<>(m_Props);
        consumer.subscribe(List.of(m_Topic));

        // We keep receiving new events until someone ctrl-c
        try {
            while (true) {
                ConsumerRecords<Integer, Customer> records = consumer.poll(Duration.ofMinutes(1));
                for (ConsumerRecord<Integer, Customer> record: records) {
                    System.out.println("******* Record=" + record);
                    Customer customer = record.value();
                    System.out.println("Customer ID:" + customer.getCustomerId() +
                            " and Customer Name:" + customer.getName() +
                            " and Customer Email:" + customer.getEmail());
                }
                consumer.commitSync();
                // Sleep for 5 seconds before sending the publishing message
                TimeUnit.SECONDS.sleep(wait);
            }
        } catch(Exception ex) {
            consumer.close();
            throw ex;
        }
    }

    public static void main(String [] args) {
        KafkaAvroConfluentConsumer consumer = new KafkaAvroConfluentConsumer();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        // Use Kafka Avro Deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-events-listener-group");
        // Schema Registry Location (Using Confluent Schema Registry)
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Use Specific Record or else we get Avro GenericRecord
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        consumer.setProps(props);
        try {
            consumer.receiveMessage();
        } catch(Exception ex) {
            // Exit
            System.out.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }
    }
}
