package com.kafka101.producer;

import com.kafka101.avro.AvroSerializer;
import com.kafka101.model.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.cloud.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.schema.registry.client.EnableSchemaRegistryClient;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@EnableSchemaRegistryClient
public class KafkaAvroProducer {
    private final int ID_GEN_BOUND = Integer.MAX_VALUE;
    private Random m_RandGen = new Random();
    private Properties m_Props;
    private final String m_Topic = "customerContacts";

    public void setProps(Properties props) {
        this.m_Props = props;
    }
    private Customer createNewCustomer() {
        Customer customer = Customer.newBuilder()
                .setCustomerId(m_RandGen.nextInt())
                .setName("John Doe")
                .setEmail("john.doe@gmail.com")
                .build();
        return customer;
    }

    public void publishMessage() throws InterruptedException {
        int wait = 5;
        System.out.println("Props=" + m_Props);
        KafkaProducer<Integer, Customer> producer = new KafkaProducer<Integer,
                Customer>(m_Props);

        // We keep producing new events until someone ctrl-c
        try {
            while (true) {
                Customer customer = createNewCustomer();
                System.out.println("Generated customer " +
                        customer.toString());
                ProducerRecord<Integer, Customer> record =
                        new ProducerRecord<Integer, Customer>(m_Topic, customer.getCustomerId(), customer);
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
        KafkaAvroProducer producer = new KafkaAvroProducer();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", IntegerSerializer.class);
        props.put("value.serializer", AvroSerializer.class);
        producer.setProps(props);
        try {
            producer.publishMessage();
        } catch(InterruptedException ex) {
            // Exit
            System.exit(0);
        }
    }
}
