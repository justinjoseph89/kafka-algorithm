package com.kafka.algo.runners.configreader.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author justin
 *
 */
public class KafkaProperties {

	private String appId = "DEFAULT_APP_ID";
	private String bootstrapServers = "localhost:9092";
	private String bootstrapServersTarget = "localhost:9092";
	private String zookeeperHost = "localhost";
	private String defaultKeySerde = "String";
	private String defaultValueSerde = "String";
	private String appVersion = "0";
	private long appDeltaValue = 100L;
	private long appSmallDeltaValue = 1L;
	private long appSleepTimeMs = 100;
	private String schemaRegistyUrl = "http://localhost:8081/";
	private String autoOffsetReset = "earliest";
	private Map<String, String> topics = new HashMap<>();
	private Map<String, String> topicsFields = new HashMap<>();

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getBootstrapServersTarget() {
		return bootstrapServersTarget;
	}

	public void setBootstrapServersTarget(String bootstrapServersTarget) {
		this.bootstrapServersTarget = bootstrapServersTarget;
	}

	public String getZookeeperHost() {
		return zookeeperHost;
	}

	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

	public String getDefaultKeySerde() {
		return defaultKeySerde;
	}

	public void setDefaultKeySerde(String defaultKeySerde) {
		this.defaultKeySerde = defaultKeySerde;
	}

	public String getDefaultValueSerde() {
		return defaultValueSerde;
	}

	public void setDefaultValueSerde(String defaultValueSerde) {
		this.defaultValueSerde = defaultValueSerde;
	}

	public String getAppVersion() {
		return appVersion;
	}

	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}

	public long getAppDeltaValue() {
		return appDeltaValue;
	}

	public void setAppDeltaValue(long appDeltaValue) {
		this.appDeltaValue = appDeltaValue;
	}

	public long getAppSmallDeltaValue() {
		return appSmallDeltaValue;
	}

	public void setAppSmallDeltaValue(long appSmallDeltaValue) {
		this.appSmallDeltaValue = appSmallDeltaValue;
	}

	public long getAppSleepTimeMs() {
		return appSleepTimeMs;
	}

	public void setAppSleepTimeMs(long appSleepTimeMs) {
		this.appSleepTimeMs = appSleepTimeMs;
	}

	public String getSchemaRegistyUrl() {
		return schemaRegistyUrl;
	}

	public void setSchemaRegistyUrl(String schemaRegistyUrl) {
		this.schemaRegistyUrl = schemaRegistyUrl;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public Map<String, String> getTopics() {
		return topics;
	}

	public void setTopics(Map<String, String> topics) {
		this.topics = topics;
	}

	public Map<String, String> getTopicsFields() {
		return topicsFields;
	}

	public void setTopicsFields(Map<String, String> topicsFields) {
		this.topicsFields = topicsFields;
	}

}
