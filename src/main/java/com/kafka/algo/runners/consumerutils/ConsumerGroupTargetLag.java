package com.kafka.algo.runners.consumerutils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.kafkautils.KafkaConnection;

import static com.kafka.algo.runners.constants.Constants.NO_GROUP_FOUND;
import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class ConsumerGroupTargetLag<K, V> {
	private AdminClient client;
	private KafkaConfigReader configReader;
	private KafkaConsumer<K, V> consumer;
	private String inputTopicName;
	private List<String> outputTopicNameList;

	/**
	 * @param inputTopicName
	 * @param consumer
	 * @param configReader
	 * @param outputTopicNameList
	 */
	public ConsumerGroupTargetLag(final String inputTopicName, final KafkaConfigReader configReader,
			final String outputTopicNameList) {
		this.configReader = configReader;
		this.inputTopicName = inputTopicName;
		this.client = AdminClient.create(kafkaProperties());
		this.consumer = new KafkaConsumer<>(KafkaConnection.getKafkaTargetConsumerProperties(configReader));
		this.outputTopicNameList = Arrays.asList(outputTopicNameList.split(","));
	}

	/**
	 * @return
	 */
	private Properties kafkaProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.configReader.getBootstrapTargetServers());
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
	 * @param partitionInfo
	 * @return
	 */
	private Map<TopicPartition, Long> getPartitionEndOffsets(Collection<TopicPartition> partitionInfo) {
		return this.consumer.endOffsets(partitionInfo);
	}

	/**
	 * @return Returns the first consumer group that subscribed to the topic which
	 *         is stable. Performance Implications.
	 * @return consumerGroupList processing the topic
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */

	private String getConsumerGroups() {

		String consumerGroup = null;
		try {
			Iterator<ConsumerGroupListing> initialItr = this.client.listConsumerGroups().all().get().iterator();
			while (initialItr.hasNext()) {
				ConsumerGroupListing consumerGroupListing = initialItr.next();
				Iterator<Entry<TopicPartition, OffsetAndMetadata>> itr = this.client
						.listConsumerGroupOffsets(consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get()
						.entrySet().iterator();
				while (itr.hasNext()) {
					Entry<TopicPartition, OffsetAndMetadata> itrNext = itr.next();
					if (itrNext.getKey().topic().equals(this.inputTopicName)) {
						Iterator<Entry<String, KafkaFuture<ConsumerGroupDescription>>> itrInside = this.client
								.describeConsumerGroups(Arrays.asList(consumerGroupListing.groupId())).describedGroups()
								.entrySet().iterator();
						while (itrInside.hasNext()) {
							Entry<String, KafkaFuture<ConsumerGroupDescription>> describeConsumer = itrInside.next();
							if (describeConsumer.getValue().get().state().toString().equals("Stable")) {
								return consumerGroupListing.groupId();
							}
						}
					}
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}

		return consumerGroup == null ? NO_GROUP_FOUND : consumerGroup;
	}

	/**
	 * @return Returns the first consumer group that subscribed to the topic which
	 *         is stable. Performance Implications.
	 * @return consumerGroupList processing the topic
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */

	private boolean checkAllTopicsInCosumerGroup(final String groupId) {

		boolean containsAll = false;
		try {
			final Set<String> consumingTopicSet = new HashSet<String>();
			this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().keySet()
					.forEach(topicPartition -> {
						consumingTopicSet.add(topicPartition.topic());
					});
			containsAll = consumingTopicSet.containsAll(this.outputTopicNameList) ? true : false;

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return containsAll;
	}

	/**
	 * @return This will return all consumer groups as a List<String> that
	 *         subscribed to the topic. Possible Performance Issue because of entire
	 *         iteration of consumer groups.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private List<String> getConsumerGroupListContainsInputTopics() {

		final List<String> consumerGroupList = new ArrayList<String>();
		try {
			this.client.listConsumerGroups().all().get().forEach(consumerGroupListing -> {
				final String groupId = consumerGroupListing.groupId();
				if (checkAllTopicsInCosumerGroup(groupId)) {
					this.client.describeConsumerGroups(Arrays.asList(groupId)).describedGroups().values()
							.forEach(consumerDesc -> {
								try {
									if (consumerDesc.get().state().toString().equals("Stable")) {
										consumerGroupList.add(groupId);
									}
								} catch (InterruptedException | ExecutionException e) {
									e.printStackTrace();
								}
							});
					consumerGroupList.add(consumerGroupListing.groupId());
				}
			});
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return consumerGroupList;
	}

	private Map<String, HashMap<String, Long>> getConsumerLagMap() {
		final List<String> activeConsumerGroupList = getConsumerGroupListContainsInputTopics();
		final Map<String, HashMap<String, Long>> hmFinal = new HashMap<String, HashMap<String, Long>>();
		activeConsumerGroupList.forEach(groupId -> {
			HashMap<String, Long> hm = new HashMap<String, Long>();
			try {
				Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = getOffsetMetadata(groupId);
				Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(consumerGroupOffsets.keySet());

				Iterator<Entry<TopicPartition, OffsetAndMetadata>> consumerItr = consumerGroupOffsets.entrySet()
						.iterator();
				while (consumerItr.hasNext()) {
					Entry<TopicPartition, OffsetAndMetadata> partitionData = consumerItr.next();
					final String topicName = partitionData.getKey().topic();
					if (!topicName.equals(this.inputTopicName + "-temp")) {
						long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
						if (lag < 0) {
							lag = 0;
						}

						if (hm.containsKey(topicName)) {
							long totalLag = hm.get(topicName) + lag;
							hm.put(topicName, totalLag);
						} else {
							hm.put(topicName, lag);
						}
					}
				}
				hmFinal.put(groupId, hm);
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		return hmFinal;

	}

	/**
	 * @return This will return all consumer groups as a List<String> that
	 *         subscribed to the topic. Possible Performance Issue because of entire
	 *         iteration of consumer groups.
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Deprecated
	private List<String> getConsumerGroupList() {

		List<String> consumerGroupList = new ArrayList<String>();
		try {
			Iterator<ConsumerGroupListing> initialItr = this.client.listConsumerGroups().all().get().iterator();
			while (initialItr.hasNext()) {
				ConsumerGroupListing consumerGroupListing = initialItr.next();
				Iterator<Entry<TopicPartition, OffsetAndMetadata>> itr = this.client
						.listConsumerGroupOffsets(consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get()
						.entrySet().iterator();
				while (itr.hasNext()) {
					Entry<TopicPartition, OffsetAndMetadata> itrNext = itr.next();
					if (itrNext.getKey().topic().equals(this.inputTopicName)) {
						Iterator<Entry<String, KafkaFuture<ConsumerGroupDescription>>> itrInside = this.client
								.describeConsumerGroups(Arrays.asList(consumerGroupListing.groupId())).describedGroups()
								.entrySet().iterator();
						while (itrInside.hasNext()) {
							Entry<String, KafkaFuture<ConsumerGroupDescription>> describeConsumer = itrInside.next();
							if (describeConsumer.getValue().get().state().toString().equals("Stable")) {
								consumerGroupList.add(consumerGroupListing.groupId());
							}
						}
					}
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return consumerGroupList;
	}

	/**
	 * {This method will return the lag of active consumer that prolvided in this}
	 * 
	 * @param groupId
	 * @return
	 */
	@Deprecated
	private long getConsumerGroupLag(String groupId) {

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
	 * {This method will return the lag of active consumer that provided in this as
	 * a list}
	 * 
	 * @param groupId
	 * @return
	 */
	@Deprecated
	private long getConsumerGroupsLag(List<String> groupIdList) {
		TreeSet<Long> lagSet = new TreeSet<Long>();
		try {
			Iterator<String> groupItr = groupIdList.iterator();
			while (groupItr.hasNext()) {
				String groupId = groupItr.next();
				long totalLag = 0L;

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
				lagSet.add(totalLag);

			}

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return lagSet.first();
	}

	/**
	 * This will return lag of any one of the consumer group that is active for this
	 * topic. This will be 0 if there is no groups available.
	 * 
	 * @return
	 */
	@Deprecated
	public long getAnyActiveConsumerLag() {
		String groupId = getConsumerGroups();
		return groupId.equals(NO_GROUP_FOUND) ? 0L : getConsumerGroupLag(groupId);
	}

	/**
	 * This will return lag of of the consumer group that is active and less in any
	 * of all for this topic. This will be 0 if there is no groups available.
	 * 
	 * @return
	 */
	@Deprecated
	public long getAnyActiveLeastConsumerLag() {
		List<String> groupIdList = getConsumerGroupList();
		return groupIdList.size() == 0 ? 0L : getConsumerGroupsLag(groupIdList);
	}

	/**
	 * This will return lag of of the consumer group that is active and less in any
	 * of all for this topic. This will be 0 if there is no groups available.
	 * 
	 * @return
	 */
	public long getTargetActiveConsumerLag() {
		Map<String, HashMap<String, Long>> groupIdList = getConsumerLagMap();
		if (groupIdList.size() < 1) {
			return 0L;
		}
		final TreeSet<Long> versionSet = new TreeSet<Long>();
		groupIdList.keySet().forEach(key -> {
			String[] keySplitor = key.split("-");
			versionSet.add(Long.parseLong(keySplitor[keySplitor.length - 1]));
		});
		final String consumerGroup = GROUPID_PREFIX + versionSet.last();
		final long maxLag = Collections.max(groupIdList.get(consumerGroup).values());
		return maxLag;
	}

	// public static void main(String[] args) {
	// KafkaConfigReader config = new
	// KafkaConfigReader("src\\main\\resources\\application.yaml");
	// ConsumerGroupTargetLag lag = new ConsumerGroupTargetLag<>("input-topic-1",
	// config, "");
	// System.out.println(lag.getTargetActiveConsumerLag(
	// Arrays.asList("Mirror-Maker-1-511589661357727,Mirror-Maker-1-541589662039208".split(","))));
	// // System.out.println(lag.getConsumerGroups());
	// //
	// System.out.println(lag.getConsumerGroupLag("Mirror-Maker-1-511589661357727"));
	//
	// }

}
