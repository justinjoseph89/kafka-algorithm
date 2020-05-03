package com.kafka.algo.runners.utils;

import static com.kafka.algo.runners.constants.Constants.BROKER_LIST;

import java.util.Properties;

/**
 * @author justin
 *
 */
public class KafkaConnection {

	/**
	 * @param consumerID
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaJsonConsumerProperties(String consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
		props.put("group.id", consumerID);
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	/**
	 * @return
	 */
	public static Properties getKafkaSimpleProducerProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}
