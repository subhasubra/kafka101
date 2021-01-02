package com.kafka101.producer;

import com.kafka101.model.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Producer that uses Avro Message Format and uses Confluent Schema Registry and Kafka provided Avro Serializer
 */
public class KafkaAvroConfluentProducer {
    private final Random m_RandGen = new Random();
    private Properties m_Props;
    private final String m_Topic = "customerContacts";

    public void setProps(Properties props) {
        this.m_Props = props;
    }
    private Customer createNewCustomer() {
        return Customer.newBuilder()
                .setCustomerId(m_RandGen.nextInt())
                .setName("John Doe")
                .setEmail("john.doe@gmail.com")
                .build();
    }

    public void publishMessage() throws InterruptedException {
        int wait = 5;
        System.out.println("Props=" + m_Props);
        KafkaProducer<Integer, Customer> producer = new KafkaProducer<>(m_Props);

        // We keep producing new events until someone ctrl-c
        try {
            while (true) {
                Customer customer = createNewCustomer();
                System.out.println("Generated customer " +
                        customer.toString());
                ProducerRecord<Integer, Customer> record =
                        new ProducerRecord<>(m_Topic, customer.getCustomerId(), customer);
                producer.send(record);
                // Sleep for 5 seconds before sending the publishing message
                TimeUnit.SECONDS.sleep(wait);
            }
        } catch(InterruptedException ex) {
            producer.close();
            throw ex;
        }
    }

    public static void main(String [] args) {
        KafkaAvroConfluentProducer producer = new KafkaAvroConfluentProducer();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        // Use Kafka Avro Deserializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        // Confluent Schema Registry Location
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producer.setProps(props);
        try {
            producer.publishMessage();
        } catch(InterruptedException ex) {
            // Exit
            System.exit(0);
        }
    }
}