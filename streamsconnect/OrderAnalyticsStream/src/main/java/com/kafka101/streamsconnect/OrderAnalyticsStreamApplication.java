package com.kafka101.streamsconnect;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.text.SimpleDateFormat;
//import java.time.Duration;
import java.util.Calendar;
import java.util.function.Function;

/**
 * SpringBoot Kafka Streams Processing Application
 * Application processes orders and generates metrics that can be consumed (Eg: by Elastic Sink Connector)
 * Uses Spring Cloud Stream and Spring Cloud Stream Kafka Stream Binder
 */
@SpringBootApplication
@Slf4j
public class OrderAnalyticsStreamApplication {

	private static final Logger logger = LoggerFactory.getLogger(OrderAnalyticsStreamApplication.class.getName());

	// Local State store name
	private final static String STORE_NAME = "order-metrics-store";

	// For querying local state store
	@Autowired
	private InteractiveQueryService interactiveQueryService;

	// Bean for Kafka Stream Processing that counts the number of orders and max order amount for a day
	@Bean
	public Function<KStream<String, T_Order>, KStream<String, OrderMetrics>> process() {
		String metricKey = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
		return input -> input
				.map((key, value) -> new KeyValue<String, T_Order>(metricKey, value))
				.groupByKey(Grouped.with(new Serdes.StringSerde(), new JsonSerde<>(T_Order.class)))
				//.windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))  // Uncomment for windowing but needs a change of type for the state store
				.<OrderMetrics>aggregate(() -> new OrderMetrics(),
						(key, order, metrics) -> {
							// Get the current metrics data from the store
							ReadOnlyKeyValueStore<String, OrderMetrics> orderMetricsStore = interactiveQueryService.getQueryableStore(STORE_NAME,
																					QueryableStoreTypes.<String, OrderMetrics>keyValueStore());
							OrderMetrics current = orderMetricsStore.get(metricKey);
							if (current == null)
								logger.error("Order Metrics State Store is empty for {}", metricKey);
							else
								logger.info("Last Updated Metrics for {} is {}", metricKey, current.toString());
							return metrics.update(order, current);
						},
						Materialized.<String, OrderMetrics, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
								.withKeySerde(new Serdes.StringSerde())
								.withValueSerde(new JsonSerde<>(OrderMetrics.class)))
				.toStream()
				.map((key, value) -> new KeyValue<String, OrderMetrics>(metricKey, value));
	}

	public static void main(String[] args) {
		SpringApplication.run(OrderAnalyticsStreamApplication.class, args);
	}

}
