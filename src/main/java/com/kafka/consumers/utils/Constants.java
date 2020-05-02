package com.kafka.consumers.utils;

/**
 * @author justin
 *
 */
public interface Constants {
	static final String GROUPID_PREFIX = "Mirror-Maker-1-";
	static final String BROKER_LIST = "localhost:9092";
	static final String ZOOKEEPER_HOST = "localhost";
	static final String ZNODE_PREFIX = "/kafka-algo_";
	static final long DELTA = 200L;
	static final long SMALL_DELTA = 100L;

}
