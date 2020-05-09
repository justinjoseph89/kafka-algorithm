package com.kafka.algo.runner.configreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kafka.algo.runner.configreader.config.KafkaConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author justin
 *
 */
public class KafkaConfigReader {
	private ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	private KafkaConfiguration kafkaConfig;

	public KafkaConfigReader() {
		try {
			this.kafkaConfig = mapper.readValue(new File("src/main/resources/application.yaml"),
					KafkaConfiguration.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return
	 */
	public String getAppId() {
		return this.kafkaConfig.getKafka().getAppId();
	}

	/**
	 * @return
	 */
	public String getBootstrapServers() {
		return this.kafkaConfig.getKafka().getBootstrapServers();
	}

	/**
	 * @return
	 */
	public String getZookeeperHost() {
		return this.kafkaConfig.getKafka().getZookeeperHost();
	}

	/**
	 * @return
	 */
	public String getDefaultKeySerde() {
		return this.kafkaConfig.getKafka().getDefaultKeySerde();
	}

	/**
	 * @return
	 */
	public String getDefaultValueSerde() {
		return this.kafkaConfig.getKafka().getDefaultValueSerde();
	}

	/**
	 * @return
	 */
	public String getAppVersion() {
		return this.kafkaConfig.getKafka().getAppVersion();
	}

	/**
	 * @return
	 */
	public long getDeltaValue() {
		return this.kafkaConfig.getKafka().getAppDeltaValue();
	}

	/**
	 * @return
	 */
	public long getSmallDeltaValue() {
		return this.kafkaConfig.getKafka().getAppSmallDeltaValue();
	}

	/**
	 * @return
	 */
	public long getSleepTimeMs() {
		return this.kafkaConfig.getKafka().getAppSleepTimeMs();
	}

	/**
	 * @return
	 */
	public Map<String, String> getTopics() {
		return this.kafkaConfig.getKafka().getTopics();
	}

	/**
	 * @return
	 */
	public String getSchemaRegistryUrl() {
		return this.kafkaConfig.getKafka().getSchemaRegistyUrl();
	}

	/**
	 * @return
	 */
	public String getAutoOffsetReset() {
		return this.kafkaConfig.getKafka().getAutoOffsetReset();
	}

}
