package com.kafka101.SchemaRegistryServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.schema.registry.EnableSchemaRegistryServer;

@SpringBootApplication
@EnableSchemaRegistryServer
public class SchemaRegistryServerApplication {

	public static void main(String[] args) {

		SpringApplication.run(SchemaRegistryServerApplication.class, args);
	}

}

