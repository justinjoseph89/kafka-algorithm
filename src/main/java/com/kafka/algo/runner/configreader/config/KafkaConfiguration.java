package com.kafka.algo.runner.configreader.config;

/**
 * @author justin
 *
 */
public class KafkaConfiguration {
	private KafkaProperties kafka;

	public KafkaProperties getKafka() {
		return kafka;
	}

	public void setKafka(KafkaProperties kafka) {
		this.kafka = kafka;
	}
}
