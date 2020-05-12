package com.kafka.algo.runners.kafkautils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.deserializers.KafkaDeserializers;
import com.kafka.algo.runners.serializers.KafkaSerializers;

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
	public static Properties getKafkaConsumerProperties(final String consumerID, final KafkaConfigReader configReader) {
		Properties props = getSharedConsumerProperties(configReader);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerID);

		return props;
	}

	/**
	 * @param consumerID
	 * @param configReader
	 * @return Properties with kafka details
	 */
	public static Properties getKafkaTargetConsumerProperties(final KafkaConfigReader configReader) {
		Properties props = getSharedConsumerProperties(configReader);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getBootstrapTargetServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerLagFinderGroup");

		return props;
	}

	/**
	 * @param configReader
	 * @return common kafka Properties
	 */
	private static Properties getSharedConsumerProperties(final KafkaConfigReader configReader) {
		Properties props = new Properties();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaDeserializers.getKeyDeserializer(configReader));
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				KafkaDeserializers.getValueDeserializer(configReader));
		props.put("schema.registry.url", configReader.getSchemaRegistryUrl());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configReader.getAutoOffsetReset());

		return props;

	}

	/**
	 * @param configReader
	 * @return
	 */
	public static Properties getKafkaProducerProperties(final KafkaConfigReader configReader) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configReader.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaSerializers.getKeySerializer(configReader));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSerializers.getValueSerializer(configReader));
		props.put("schema.registry.url", configReader.getSchemaRegistryUrl());
		return props;
	}

}
