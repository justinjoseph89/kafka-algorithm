package com.kafka.consumers.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerGroupLag<K, V> {
	private KafkaConsumer<K, V> consumer;
	private final String brokers = "192.168.56.101:9092";
	AdminClient client;

	/**
	 * @param consumer
	 */
	public ConsumerGroupLag(KafkaConsumer<K, V> consumer) {
		this.consumer = consumer;
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		this.client = AdminClient.create(props);
	}

	/**
	 * @return
	 */
	private Properties kafkaProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		return props;
	}

	/**
	 * @param groupId
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private Map<TopicPartition, OffsetAndMetadata> getOffsetMetadata(String groupId)
			throws InterruptedException, ExecutionException {
		System.out.println(client.listConsumerGroups().all());
		System.out.println(this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
		return this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
	}

	/**
	 * @param partitionInfo
	 * @return
	 */
	private Map<TopicPartition, Long> getPartitionEndOffsets(Collection<TopicPartition> partitionInfo) {
		return this.consumer.endOffsets(partitionInfo);
	}

	/**
	 * @param groupId
	 * @return
	 */
	public long getConsumerGroupLag(String groupId) {

		long totalLag = 0L;
		try {
			Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = getOffsetMetadata(groupId);
			Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(consumerGroupOffsets.keySet());
			Iterator<Entry<TopicPartition, OffsetAndMetadata>> consumerItr = consumerGroupOffsets.entrySet().iterator();
			while (consumerItr.hasNext()) {
				Entry<TopicPartition, OffsetAndMetadata> partitionData = consumerItr.next();

				long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
				if (lag < 0) {
					lag = 0;
				}
				totalLag = totalLag + lag;
			}

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return totalLag;
	}

}
