package com.kafka101.streamsconnect.logdatasource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootApplication
@EnableAsync
public class LogdatasourceApplication {

	private class LogGen implements Runnable {

		private Logger logger = Logger.getLogger(LogdatasourceApplication.class.getName());

		private String logFileName = "/usr/local/temp/order-logs.log";

		private Random randGen = new Random();

		private void logSuccessMessage() throws IOException {
			ObjectMapper objectMapper = new ObjectMapper();
			ObjectNode orderObj = objectMapper.createObjectNode();
			orderObj.put("orderId", UUID.randomUUID().toString());
			orderObj.put("status", "CREATED");
			orderObj.put("amount", randGen.nextInt(10000));
			orderObj.put("customerId", UUID.randomUUID().toString());
			String message = objectMapper.writeValueAsString(orderObj) + "\n";
			Files.write(Paths.get(logFileName), message.getBytes(), StandardOpenOption.APPEND);
			logger.log(Level.INFO, "Order Successfully Placed. Order Details:" + message);

		}

		private void logErrorMessage() throws IOException {
			ObjectMapper objectMapper = new ObjectMapper();
			ObjectNode errorObj = objectMapper.createObjectNode();
			errorObj.put("ERROR_CODE", 500);
			errorObj.put("ERROR_MSG", "Unknown Error");
			errorObj.put("ERROR_TYPE", "SEVERE");
			String message = objectMapper.writeValueAsString(errorObj) + "\n";
			Files.write(Paths.get(logFileName), message.getBytes(), StandardOpenOption.APPEND);
			logger.log(Level.SEVERE, "Order Creation Failed. Error Details:" + message);
		}

		@Override
		public void run() {
			try {
				int count = 0;
				while (true) {
					if (count == -1) {
						logErrorMessage();
						count = 0;
					} else {
						logSuccessMessage();
						count++;
					}
					Thread.sleep(10000); // sleep for 1 min
				}
			} catch (Exception ex) {
				System.out.println("Exception:" + ex.getMessage());
			}
		}
	}

	@Bean
	public TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

	@Bean
	public CommandLineRunner schedulingRunner(TaskExecutor executor) {
		return new CommandLineRunner() {
			@Override
			public void run(String... args) throws Exception {
				executor.execute(new LogGen());
			}
		};
	}

	public static void main(String[] args) {

		SpringApplication.run(LogdatasourceApplication.class, args);
	}

}
