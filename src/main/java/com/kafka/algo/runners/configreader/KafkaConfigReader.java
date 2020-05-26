package com.kafka.algo.runners.configreader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kafka.algo.KafkaAlgoApp;
import com.kafka.algo.runners.configreader.config.KafkaConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author justin
 *
 */
public class KafkaConfigReader {
	private static final Logger LOGGER = Logger.getLogger(KafkaConfigReader.class.getName());

	private ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
	private KafkaConfiguration kafkaConfig;

	public KafkaConfigReader(String path) {
		try {
			this.kafkaConfig = mapper.readValue(new File(path), KafkaConfiguration.class);
		} catch (FileNotFoundException e) {
			LOGGER.error("File Not Found Exception While Reading YAML file");
		} catch (IOException e) {
			LOGGER.error("IOException While Reading YAML file ");
		}
	}

//	/**
//	 * @return
//	 */
//	public String getAppId() {
//		return this.kafkaConfig.getKafka().getAppId();
//	}

	/**
	 * @return
	 */
	public String getBootstrapServers() {
		return this.kafkaConfig.getKafka().getBootstrapServers();
	}

	/**
	 * @return
	 */
	public String getBootstrapTargetServers() {
		return this.kafkaConfig.getKafka().getBootstrapServersTarget();
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
	public float getDeltaValue() {
		return this.kafkaConfig.getKafka().getAppDeltaValue();
	}

	/**
	 * @return
	 */
	public float getSmallDeltaValue() {
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

	/**
	 * @return
	 */
	public long getConsumerThreads() {
		return this.kafkaConfig.getKafka().getNumConsumerThreads();
	}

	/**
	 * @return
	 */
	public Map<String, String> getTopicFields() {
		return this.kafkaConfig.getKafka().getTopicsFields();
	}

	public boolean isZkNodeUpd() {
		return this.kafkaConfig.getKafka().isZkNodeUpd();
	}
	
}
