package com.kafka.algo.runners.utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import com.kafka.algo.runner.configreader.KafkaConfigReader;

/**
 * Use the functions in this class to identify the kafka related data
 * 
 * @author justin
 *
 */
public class KafkaUtils {

	final KafkaConsumer<String, String> consumer;
	final String topicName;
	final Map<Integer, Long> topicTimestampMap;
	final KafkaConfigReader configReader;

	/**
	 * @param topicName
	 * @param configReader
	 */
	public KafkaUtils(final String topicName, final KafkaConfigReader configReader) {
		this.configReader = configReader;
		this.consumer = createNewKafkaConsumer();
		this.topicName = topicName;
		this.topicTimestampMap = getPartitionsMinimumTime();
	}

	/**
	 * @return
	 */
	private KafkaConsumer<String, String> createNewKafkaConsumer() {
		return new KafkaConsumer<String, String>(
				KafkaConnection.getKafkaJsonConsumerProperties("" + System.currentTimeMillis(), this.configReader));
	}

	/**
	 * @return
	 */
	private Map<Integer, Long> getPartitionsMinimumTime() {
		Map<Integer, Long> timestampMap = new HashMap<Integer, Long>();

		this.consumer.subscribe(Arrays.asList(this.topicName));
		ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(10000));

		for (ConsumerRecord<String, String> rec : records) {
			if (!timestampMap.containsKey(rec.partition())) {
				timestampMap.put(rec.partition(), rec.timestamp());
			}
			if (timestampMap.size() == getNumberOfTopicPartition()) {
				break;
			}
		}
		return timestampMap;
	}

	/**
	 * @return
	 */
	public long getTopicMinimumTime() {
		return this.topicTimestampMap.isEmpty() ? 0L : Collections.min(this.topicTimestampMap.values());
	}

	/**
	 * @param partition
	 * @return
	 */
	public long getTopicPartitionMinimumTime(int partition) {
		return this.topicTimestampMap.containsKey(partition) ? this.topicTimestampMap.get(partition) : 0L;
	}

	/**
	 * @return
	 */
	private List<PartitionInfo> getPartitionInfo() {
		return this.consumer.partitionsFor(this.topicName);
	}

	/**
	 * @return
	 */
	private int getNumberOfTopicPartition() {
		return getPartitionInfo().size();
	}

}
