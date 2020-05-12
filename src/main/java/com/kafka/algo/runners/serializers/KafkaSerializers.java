package com.kafka.algo.runners.serializers;

import com.kafka.algo.runners.configreader.KafkaConfigReader;

/**
 * @author justin
 *
 */
public class KafkaSerializers {

	/**
	 * @param configReader
	 * @return
	 */
	public static String getKeySerializer(final KafkaConfigReader configReader) {
		String keyDeserializerName;

		switch (configReader.getDefaultKeySerde()) {
		case "String":
			keyDeserializerName = "org.apache.kafka.common.serialization.StringSerializer";
			break;
		case "Long":
			keyDeserializerName = "org.apache.kafka.common.serialization.LongSerializer";
			break;
		case "Integer":
			keyDeserializerName = "org.apache.kafka.common.serialization.IntegerSerializer";
			break;
		case "ByteArray":
			keyDeserializerName = "org.apache.kafka.common.serialization.ByteArraySerializer";
			break;
		case "Avro":
			keyDeserializerName = "io.confluent.kafka.serializers.KafkaAvroSerializer";
			break;
		default:
			keyDeserializerName = "io.confluent.kafka.serializers.KafkaAvroSerializer";
			break;
		}
		return keyDeserializerName;

	}

	/**
	 * @param configReader
	 * @return
	 */
	public static String getValueSerializer(final KafkaConfigReader configReader) {

		String valueDeserializerName;

		switch (configReader.getDefaultValueSerde()) {
		case "String":
			valueDeserializerName = "org.apache.kafka.common.serialization.StringSerializer";
			break;
		case "Long":
			valueDeserializerName = "org.apache.kafka.common.serialization.LongSerializer";
			break;
		case "Integer":
			valueDeserializerName = "org.apache.kafka.common.serialization.IntegerSerializer";
			break;
		case "ByteArray":
			valueDeserializerName = "org.apache.kafka.common.serialization.ByteArraySerializer";
			break;
		case "Avro":
			valueDeserializerName = "io.confluent.kafka.serializers.KafkaAvroSerializer";
			break;
		default:
			valueDeserializerName = "io.confluent.kafka.serializers.KafkaAvroSerializer";
			break;
		}
		return valueDeserializerName;
	}

}
