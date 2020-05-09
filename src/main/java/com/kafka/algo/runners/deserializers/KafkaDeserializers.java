package com.kafka.algo.runners.deserializers;

import com.kafka.algo.runner.configreader.KafkaConfigReader;

/**
 * @author justin
 *
 */
public class KafkaDeserializers {

	/**
	 * @param configReader
	 * @return
	 */
	public static String getKeyDeserializer(final KafkaConfigReader configReader) {
		String keyDeserializerName;

		switch (configReader.getDefaultKeySerde()) {
		case "String":
			keyDeserializerName = "org.apache.kafka.common.serialization.StringDeserializer";
			break;
		case "Long":
			keyDeserializerName = "org.apache.kafka.common.serialization.LongDeserializer";
			break;
		case "Integer":
			keyDeserializerName = "org.apache.kafka.common.serialization.IntegerDeserializer";
			break;
		case "ByteArray":
			keyDeserializerName = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
			break;
		case "Avro":
			keyDeserializerName = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
			break;
		default:
			keyDeserializerName = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
			break;
		}
		return keyDeserializerName;

	}

	public static String getValueDeserializer(final KafkaConfigReader configReader) {

		String valueDeserializerName;

		switch (configReader.getDefaultValueSerde()) {
		case "String":
			valueDeserializerName = "org.apache.kafka.common.serialization.StringDeserializer";
			break;
		case "Long":
			valueDeserializerName = "org.apache.kafka.common.serialization.LongDeserializer";
			break;
		case "Integer":
			valueDeserializerName = "org.apache.kafka.common.serialization.IntegerDeserializer";
			break;
		case "ByteArray":
			valueDeserializerName = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
			break;
		case "Avro":
			valueDeserializerName = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
			break;
		default:
			valueDeserializerName = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
			break;
		}
		return valueDeserializerName;
	}

}
