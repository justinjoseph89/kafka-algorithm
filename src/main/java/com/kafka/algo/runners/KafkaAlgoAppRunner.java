package com.kafka.algo.runners;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.consumerutils.ConsumerGroupSourceLag;
import com.kafka.algo.runners.consumerutils.ConsumerGroupTargetLag;
import com.kafka.algo.runners.datautils.FieldSelector;
import com.kafka.algo.runners.kafkautils.KafkaConnection;
import com.kafka.algo.runners.kafkautils.KafkaUtils;
import com.kafka.algo.runners.utils.ZkConnect;

/**
 * @author justin
 *
 */
public class KafkaAlgoAppRunner<K, V> {
  private double maxTime = 0d;
  private double leadTime = 0d; // Initial Assignment
  private String inputTopicName;
  private String outputTopicName;
  private String groupId;
  private KafkaConfigReader configReader;
  private FieldSelector fieldSelector;
  private String outputTopicNameList;
  private KafkaUtils<K, V> kafkaUtils;
  private boolean isZkNodeUpd;
  private KafkaConsumer<K, V> consumer;

  public KafkaAlgoAppRunner(final String inputTopicName, final int partitionNumber,
      final KafkaConfigReader configReader, final String inputTopicNameList, final KafkaConsumer<K, V> consumer) {
    this.inputTopicName = inputTopicName;
    this.outputTopicName = inputTopicName;
    this.configReader = configReader;
    this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion();
    this.fieldSelector = new FieldSelector();
    this.outputTopicNameList = inputTopicNameList;
    this.kafkaUtils = new KafkaUtils<K, V>(this.inputTopicName, this.configReader);
    this.isZkNodeUpd = this.configReader.isZkNodeUpd();
    this.consumer = consumer;
  }

  public void start() {
    final ZkConnect zk = new ZkConnect(this.inputTopicName, isZkNodeUpd, this.configReader);
    final ConsumerGroupTargetLag<K, V> targetLag = new ConsumerGroupTargetLag<K, V>(this.inputTopicName,
        this.configReader, this.outputTopicNameList);

    final ConsumerGroupSourceLag<K, V> consumerLag = new ConsumerGroupSourceLag<K, V>(this.consumer, this.configReader);
    final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
        KafkaConnection.getKafkaProducerProperties(this.configReader));

//    consumer.subscribe(java.util.Arrays.asList(this.inputTopicName));
    final String[] fieldNameToConsider = this.configReader.getTopicFields().get(this.inputTopicName).split(",");

    boolean considerLag = false;

    while (true) {
      final ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
      boolean checkFlag = true;
      for (ConsumerRecord<K, V> rec : records) {
        checkFlag = messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);
        System.out.println("topic--" + rec.topic() + "-" + rec.partition() + "-" + rec.offset() + "-" + checkFlag);
        while (checkFlag) {
          checkFlag = messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);
        }
      }
      considerLag = true;
    }
  }

  private boolean messageSendRecursive(final ConsumerRecord<K, V> rec, final KafkaProducer<K, V> producer,
      final ConsumerGroupSourceLag<K, V> consumerLag, final boolean considerLag, final ZkConnect zk,
      final String[] fieldNameToConsider, final ConsumerGroupTargetLag<K, V> targetLag) {

    boolean statusFlag = true;
    final long recTimestamp = this.fieldSelector.getTimestampFromData(rec, fieldNameToConsider);
    // lag of partition
    final long lag = consumerLag.getConsumerGroupLag(this.groupId, this.inputTopicName, rec);
    // max lag of consumers from the target
    final long maxLag = targetLag.getTargetActiveConsumerLag();

    if (maxTime == 0d) {
      maxTime = zk.getMinimum() == 0d ? recTimestamp : zk.getMinimum();
      maxTime = maxTime + this.configReader.getDeltaValue();
      zk.updateNode(maxTime, rec.partition());
    }
    if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
      if (lag == 0 && considerLag == true) {
        this.leadTime = 0;
      } else {
        this.leadTime = recTimestamp - maxTime;
      }

      if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
        producer.send(new ProducerRecord<K, V>(this.outputTopicName, rec.partition(), rec.key(), rec.value()));
        this.consumer.commitSync(Collections.singletonMap(new TopicPartition(this.outputTopicName, rec.partition()),
            new OffsetAndMetadata(rec.offset())));
        statusFlag = false;
      }
    }

    if (this.leadTime > this.configReader.getSmallDeltaValue() && statusFlag == true) {
      zk.updateNode(maxTime, rec.partition());
      while (zk.getMinimum(consumerLag.getTopicPartitionWithZeroLag(this.groupId)) < maxTime
          || maxLag > this.configReader.getSmallDeltaValue()) {
        try {
          Thread.sleep(this.configReader.getSleepTimeMs());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      maxTime = maxTime + this.configReader.getDeltaValue();
      this.leadTime = 0;
      statusFlag = true;
    }
    return statusFlag;
  }

}
