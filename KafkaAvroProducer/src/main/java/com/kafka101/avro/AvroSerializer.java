package com.kafka101.avro;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.cloud.schema.registry.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.schema.registry.avro.DefaultSubjectNamingStrategy;
import org.springframework.cloud.schema.registry.client.DefaultSchemaRegistryClient;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

public class AvroSerializer  implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        AvroSchemaRegistryClientMessageConverter serializer = new AvroSchemaRegistryClientMessageConverter(
                new DefaultSchemaRegistryClient(new RestTemplate()),
                new NoOpCacheManager(),
                new AvroSchemaServiceManagerImpl());
        serializer.setSubjectNamingStrategy(new DefaultSubjectNamingStrategy());
        Message<?> message = MessageBuilder.withPayload(data).build();
        Map<String, Object> headers = new HashMap<>(message.getHeaders());
        return (byte[]) serializer.toMessage(message.getPayload(), new MessageHeaders(headers)).getPayload();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return serialize(topic, data);
    }

    @Override
    public void close() {
        // Do Nothing
    }
}
