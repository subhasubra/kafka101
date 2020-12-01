package com.kafka101.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("dev") // used only for dev environment
public class KafkaConfig {
	
	@Bean
	public NewTopic orderEventTopic() {
		return TopicBuilder.name("order-events-topic").build();
	}

}
