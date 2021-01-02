package com.kafka101.avro;

import com.kafka101.model.Customer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.schema.registry.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.schema.registry.avro.DefaultSubjectNamingStrategy;
import org.springframework.cloud.schema.registry.client.DefaultSchemaRegistryClient;
import org.springframework.core.io.UrlResource;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.web.client.RestTemplate;
import java.util.Map;

/**
 * Custom Avro Deserializer
 */

public class AvroDeserializer implements Deserializer {

    @Override
    public void configure(Map configs, boolean isKey) {
        // Do Nothing
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        AvroSchemaRegistryClientMessageConverter deserializer = new AvroSchemaRegistryClientMessageConverter(
                new DefaultSchemaRegistryClient(new RestTemplate()),
                new NoOpCacheManager(),
                new AvroSchemaServiceManagerImpl());
        deserializer.setSubjectNamingStrategy(new DefaultSubjectNamingStrategy());

        try {
            UrlResource [] urls = {new UrlResource ("http://localhost:8990/customer/avro")};
            deserializer.setSchemaLocations(urls);
            //ClassPathResource [] schemaResources = {new ClassPathResource("avro/customer-v1.avsc")};
            //deserializer.setSchemaLocations(schemaResources);
            deserializer.afterPropertiesSet();
        } catch (Exception e) {
            System.out.println("AvroDeserializer:deserialize Error:" + e.toString());
            //throw new SerializationFailedException(e.getMessage());
        }
        Message message = MessageBuilder.withPayload(data).build();
                            //.setHeader("contentType", "application/avro").build();

        final Object obj = deserializer.fromMessage(message, Customer.class, new MimeType("application", "*+avro"));
        //final Object obj = deserializer.fromMessage(message, Customer.class);
        System.out.println("******Deserialized Object:" + obj.toString());
        return obj;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public void close() {
        // Do Nothing
    }
}
