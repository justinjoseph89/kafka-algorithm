package com.kafka.consumers.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import static com.kafka.consumers.utils.Constants.GROUPID_PREFIX;
import static com.kafka.consumers.utils.Constants.BROKER_LIST;;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class ConsumerGroupLag<K, V> {
	private KafkaConsumer<K, V> consumer;
	private AdminClient client;

	/**
	 * @param consumer
	 */
	public ConsumerGroupLag(KafkaConsumer<K, V> consumer) {
		this.consumer = consumer;
		this.client = AdminClient.create(kafkaProperties());
	}

	/**
	 * @return
	 */
	private Properties kafkaProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER_LIST);
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
		return this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
	}

	/**
	 * @param groupIdStart
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@SuppressWarnings("unused")
	@Deprecated
	private Map<TopicPartition, OffsetAndMetadata> getGroupOffsetMetadata(String groupIdStart)
			throws InterruptedException, ExecutionException {
		Map<TopicPartition, OffsetAndMetadata> offsetMetadataMap = new HashMap<TopicPartition, OffsetAndMetadata>();
		Collection<ConsumerGroupListing> allGroupResults = this.client.listConsumerGroups().all().get();
		Iterator<ConsumerGroupListing> itsGroup = allGroupResults.iterator();
		while (itsGroup.hasNext()) {
			ConsumerGroupListing consumerGroupListing = (ConsumerGroupListing) itsGroup.next();
			String groupId = consumerGroupListing.groupId();
			if (groupId.startsWith(groupIdStart)) {
				offsetMetadataMap
						.putAll(this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get());
			}

		}
		return offsetMetadataMap;
	}

	/**
	 * @param partitionInfo
	 * @return list of end offsets for a partition in topic
	 */
	private Map<TopicPartition, Long> getPartitionEndOffsets(Collection<TopicPartition> partitionInfo) {
		return this.consumer.endOffsets(partitionInfo);
	}

	/**
	 * @param groupId
	 * @return lag for the consumer group provided
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

	/**
	 * @return maximum lag from all the consumer groups that starts with the group
	 *         id prefix
	 */
	public long getMaxConsumerGroupLag() {

		long totalLag = 0L;
		TreeSet<Long> lagSet = new TreeSet<Long>();
		try {

			Collection<ConsumerGroupListing> allGroupResults = this.client.listConsumerGroups().all().get();
			Iterator<ConsumerGroupListing> itsGroup = allGroupResults.iterator();
			while (itsGroup.hasNext()) {
				ConsumerGroupListing consumerGroupListing = (ConsumerGroupListing) itsGroup.next();
				String groupId = consumerGroupListing.groupId();
				if (groupId.startsWith(GROUPID_PREFIX)) {
					Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = getOffsetMetadata(groupId);
					Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(consumerGroupOffsets.keySet());
					Iterator<Entry<TopicPartition, OffsetAndMetadata>> consumerItr = consumerGroupOffsets.entrySet()
							.iterator();
					while (consumerItr.hasNext()) {
						Entry<TopicPartition, OffsetAndMetadata> partitionData = consumerItr.next();
						long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
						if (lag < 0) {
							lag = 0;
						}
						totalLag = totalLag + lag;
					}
				}
				lagSet.add(totalLag);
			}

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return lagSet.last();
	}

}
