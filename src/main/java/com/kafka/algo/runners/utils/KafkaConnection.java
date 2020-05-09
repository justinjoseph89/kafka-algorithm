package com.kafka.algo.runners.utils;

import java.util.Properties;

import com.kafka.algo.runner.configreader.KafkaConfigReader;

/**
 * @author justin
 *
 */
public class KafkaConnection {

	/**
	 * @param consumerID
	 * @param configReader
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaJsonConsumerProperties(final String consumerID,
			final KafkaConfigReader configReader) {
		Properties props = new Properties();
		props.put("bootstrap.servers", configReader.getBootstrapServers());
		props.put("group.id", consumerID);
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	/**
	 * @return
	 */
	public static Properties getKafkaSimpleProducerProperties(final KafkaConfigReader configReader) {
		Properties props = new Properties();
		props.put("bootstrap.servers", configReader.getBootstrapServers());
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}
