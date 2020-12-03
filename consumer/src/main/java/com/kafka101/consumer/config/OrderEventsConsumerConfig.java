package com.kafka101.consumer.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class OrderEventsConsumerConfig {
    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =  new ConcurrentKafkaListenerContainerFactory<>();

        // For Concurrent Listeners
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(4);

        //Uncomment for manual offset commit (acknowledgement)
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }
}
