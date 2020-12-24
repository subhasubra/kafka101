package com.kafka101.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
//import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
@Slf4j
public class OrderEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =  new ConcurrentKafkaListenerContainerFactory<>();

        // For Concurrent Listeners
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(4);

        // Set the Consumer Rack Id to use custom RackAwareAssignor
        /*Properties props = new Properties();
        props.put("assignment.consumer.rack", "in-east-1");
        factory.getContainerProperties().setKafkaConsumerProperties(props);*/

        //Uncomment for manual offset commit (acknowledgement)
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Custom Exception Handler
        factory.setErrorHandler((thrownException, record) -> {
            log.error("OrderEventsConsumerConfig: Exception {} occurred and the record details are {}", thrownException.getMessage(), record);
        });

        // Retries in case of failure
        factory.setRetryTemplate(getRetryTemplate());

        // Recovery Strategy
        factory.setRecoveryCallback(context -> {
            if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                // Recovery logic goes here. For now just log.
                log.info("Executing Recovery Logic");
            } else {
                // Non Recoverable logic goes here. For now just log.
                log.info("Executing Non Recoverable Logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        });

        return factory;
    }

    // Set Retry Strategy
    private RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        // Use for very simple use case to handle retries for all exceptions
        /*
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3);
         */

        // Retries for specific exceptions
        Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap();
        exceptionMap.put(IllegalArgumentException.class, false);
        exceptionMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionMap, true);

        return simpleRetryPolicy;
    }
}
