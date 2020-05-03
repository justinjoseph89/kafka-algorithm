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

/**
 * Use the functions in this class to identify the kafka related data
 * 
 * @author justin
 *
 */
public class KafkaUtils {

	KafkaConsumer<String, String> consumer;
	String topicName;
	Map<Integer, Long> topicTimestampMap;

	/**
	 * @param topicName
	 */
	public KafkaUtils(String topicName) {
		this.consumer = createNewKafkaConsumer();
		this.topicName = topicName;
		this.topicTimestampMap = getPartitionsMinimumTime();
	}

	/**
	 * @return create a new kafka consumer
	 */
	private KafkaConsumer<String, String> createNewKafkaConsumer() {
		return new KafkaConsumer<String, String>(
				KafkaConnection.getKafkaJsonConsumerProperties("" + System.currentTimeMillis()));
	}

	/**
	 * @return get the map of minimum record time for all partitions in the topic
	 *         provided in the object creation time
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
	 * @return minimum time of records available in the topic provided while
	 *         creating the object.
	 */
	public long getTopicMinimumTime() {
		return this.topicTimestampMap.isEmpty() ? 0L : Collections.min(this.topicTimestampMap.values());
	}

	/**
	 * @param partition
	 * @return minimum time of records available in the partition provided while
	 *         creating the object.
	 */
	public long getTopicPartitionMinimumTime(int partition) {
		return this.topicTimestampMap.containsKey(partition) ? this.topicTimestampMap.get(partition) : 0L;
	}

	/**
	 * @return get the list all partition info for the topic given while creating
	 *         the object
	 */
	private List<PartitionInfo> getPartitionInfo() {
		return this.consumer.partitionsFor(this.topicName);
	}

	/**
	 * @return total number of partition in the topic given while creating the
	 *         object
	 */
	private int getNumberOfTopicPartition() {
		return getPartitionInfo().size();
	}

}
