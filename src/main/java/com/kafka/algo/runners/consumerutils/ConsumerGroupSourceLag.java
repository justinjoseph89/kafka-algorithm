package com.kafka.algo.runners.consumerutils;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.kafka.algo.runners.configreader.KafkaConfigReader;;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class ConsumerGroupSourceLag<K, V> {
  private KafkaConsumer<K, V> consumer;
  private AdminClient client;
  private KafkaConfigReader configReader;

  /**
   * @param consumer
   * @param configReader
   */
  public ConsumerGroupSourceLag(final KafkaConsumer<K, V> consumer, final KafkaConfigReader configReader,
      final AdminClient client) {
    this.consumer = consumer;
    this.configReader = configReader;
    this.client = client;
//		this.client = AdminClient.create(kafkaProperties());
  }

  /**
   * @return
   */
  private Properties kafkaProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", this.configReader.getBootstrapServers());
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
   * @param groupId
   * @param inputTopicName
   * @return
   */
  public long getConsumerGroupLag(final String groupId, final String inputTopicName, final ConsumerRecord<K, V> rec) {

    final HashMap<String, Long> finalMap = new HashMap<String, Long>();
    final String partitionIndexName = groupId + "-" + inputTopicName + "-" + rec.partition();
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
        finalMap.put(groupId + "-" + inputTopicName + "-" + partitionData.getKey().partition(), lag);
      }

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return finalMap.containsKey(partitionIndexName) ? finalMap.get(partitionIndexName) : 0L;
  }

  /**
   * @param groupId
   * @param inputTopicName
   * @return
   */
  public HashMap<String, Long> getTopicPartitionWithZeroLag(final String groupId) {

    final HashMap<String, Long> finalMap = new HashMap<String, Long>();
    final List<String> sizeList = new ArrayList<String>();
    try {
      Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = getOffsetMetadata(groupId);
      Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(consumerGroupOffsets.keySet());
      Iterator<Entry<TopicPartition, OffsetAndMetadata>> consumerItr = consumerGroupOffsets.entrySet().iterator();
      while (consumerItr.hasNext()) {
        Entry<TopicPartition, OffsetAndMetadata> partitionData = consumerItr.next();

        long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
        if (lag <= 0) {
          lag = 0;
          finalMap.put(partitionData.getKey().topic() + "-" + partitionData.getKey().partition(), lag);
        }
        sizeList.add(partitionData.getKey().topic() + "-" + partitionData.getKey().partition());
      }

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    finalMap.put("elementSize", Long.valueOf(sizeList.size()));

    return finalMap;
  }

  @Deprecated
  public long getMaxConsumerGroupLag() {

    TreeSet<Long> lagSet = new TreeSet<Long>();
    try {

      Collection<ConsumerGroupListing> allGroupResults = this.client.listConsumerGroups().all().get();
      Iterator<ConsumerGroupListing> itsGroup = allGroupResults.iterator();
      while (itsGroup.hasNext()) {
        long totalLag = 0L;
        ConsumerGroupListing consumerGroupListing = (ConsumerGroupListing) itsGroup.next();
        String groupId = consumerGroupListing.groupId();

        if (groupId.startsWith(GROUPID_PREFIX + this.configReader.getAppVersion())) {
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
        }
        lagSet.add(totalLag);
      }

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return lagSet.last();
  }

  @Deprecated
  public long getMaxConsumerGroupLag(String exsGroupId) {

    TreeSet<Long> lagSet = new TreeSet<Long>();
    try {

      Collection<ConsumerGroupListing> allGroupResults = this.client.listConsumerGroups().all().get();
      Iterator<ConsumerGroupListing> itsGroup = allGroupResults.iterator();
      while (itsGroup.hasNext()) {
        long totalLag = 0L;
        ConsumerGroupListing consumerGroupListing = (ConsumerGroupListing) itsGroup.next();
        String groupId = consumerGroupListing.groupId();

        if (groupId.startsWith(GROUPID_PREFIX + this.configReader.getAppVersion()) && !groupId.equals(exsGroupId)) {
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
        }
        lagSet.add(totalLag);
      }

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return lagSet.last();
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
        offsetMetadataMap.putAll(this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get());
      }

    }
    return offsetMetadataMap;
  }
}
