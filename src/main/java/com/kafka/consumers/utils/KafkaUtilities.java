package com.kafka.consumers.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;

import io.confluent.kafka.serializers.KafkaAvroDecoder;

public class KafkaUtilities {

	/**
	 * @param consumerID
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaAvroConsumerProperties(long consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "GroupID" + System.currentTimeMillis());
		props.put("auto.offset.reset", "earliest");
		props.put("schema.registry.url", "http://localhost:8081/");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		return props;
	}

	/**
	 * @param consumerID
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaJsonConsumerProperties(long consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", String.valueOf(consumerID));
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
	/**
	 * @param consumerID
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaJsonConsumerPropertiesLong(long consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", String.valueOf(consumerID));
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		return props;
	}

	public static Properties getKafkaAvroProducerProperties() {
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("schema.registry.url", "http://localhost:8081/");
		return props;
	}

	public static Properties getKafkaSimpleProducerProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
	public static Properties getKafkaSimpleProducerPropertiesLong() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		return props;
	}

}
