package com.kafka.algo.runners;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.ZooKeeper;

import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.consumerutils.ConsumerGroupSourceLag;
import com.kafka.algo.runners.consumerutils.ConsumerGroupTargetLag;
import com.kafka.algo.runners.datautils.FieldSelector;
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
  public FieldSelector fieldSelector;
  private String outputTopicNameList;
  private boolean isZkNodeUpd;
  private KafkaConsumer<K, V> consumer;
  private KafkaProducer<K, V> producer;
  private ConsumerGroupTargetLag<K, V> targetLag;
  private ZkConnect zk;
  private ConsumerGroupSourceLag<K, V> consumerLag;
  private String[] fieldNameToConsider;
  private boolean considerLag;

  public KafkaAlgoAppRunner(final String inputTopicName, final KafkaConfigReader configReader,
      final String inputTopicNameList, final KafkaConsumer<K, V> consumer, final KafkaProducer<K, V> producer,
      final AdminClient targetClient, final AdminClient sourceClient, final KafkaConsumer<K, V> targetKafkaConsuemr,
      final ZooKeeper zk) {
    this.inputTopicName = inputTopicName;
    this.outputTopicName = inputTopicName;
    this.configReader = configReader;
    this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion();
    this.fieldSelector = new FieldSelector();
    this.outputTopicNameList = inputTopicNameList;
    this.isZkNodeUpd = this.configReader.isZkNodeUpd();
    this.consumer = consumer;
    this.producer = producer;
    this.targetLag = new ConsumerGroupTargetLag<K, V>(this.inputTopicName, this.configReader, this.outputTopicNameList,
        targetClient, targetKafkaConsuemr);
    this.zk = new ZkConnect(this.inputTopicName, isZkNodeUpd, this.configReader, zk);
    this.consumerLag = new ConsumerGroupSourceLag<K, V>(this.consumer, this.configReader, sourceClient);
    this.fieldNameToConsider = this.configReader.getTopicFields().get(this.inputTopicName).split(",");
    this.considerLag = false;
  }

  public void start() {
    ConsumerRecords<K, V> records = callPoll();
    boolean checkFlag = true;
    for (ConsumerRecord<K, V> rec : records) {
      checkFlag = messageSendRecursive(rec, considerLag, fieldNameToConsider);
      while (checkFlag) {
        checkFlag = messageSendRecursive(rec, considerLag, fieldNameToConsider);
      }
    }
    considerLag = true;
    checkStatusFlag(checkFlag);
  }

  private void checkStatusFlag(boolean checkFlag) {
    if (checkFlag == false) {
      start();
    }
  }

  private ConsumerRecords<K, V> callPoll() {
    final ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
    return records;
  }

  private boolean messageSendRecursive(final ConsumerRecord<K, V> rec, final boolean considerLag,
      final String[] fieldNameToConsider) {

    boolean statusFlag = true;
    final long recTimestamp = this.fieldSelector.getTimestampFromData(rec, fieldNameToConsider);
    // lag of partition
    final long lag = this.consumerLag.getConsumerGroupLag(this.groupId, this.inputTopicName, rec);
    // max lag of consumers from the target
    final long maxLag = targetLag.getTargetActiveConsumerLag();

    if (maxTime == 0d) {
      maxTime = this.zk.getMinimum() == 0d ? recTimestamp : this.zk.getMinimum();
      maxTime = maxTime + this.configReader.getDeltaValue();
      this.zk.updateNode(maxTime, rec.partition());
    }
    if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
      if (lag == 0 && considerLag == true) {
        this.leadTime = 0;
      } else {
        this.leadTime = recTimestamp - maxTime;
      }

      if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
        this.producer.send(new ProducerRecord<K, V>(this.outputTopicName, rec.partition(), rec.key(), rec.value()));
        this.consumer.commitSync();
        statusFlag = false;
      }
    }

    if (this.leadTime > this.configReader.getSmallDeltaValue() && statusFlag == true) {
      this.zk.updateNode(maxTime, rec.partition());
      while (this.zk.getMinimum(this.consumerLag.getTopicPartitionWithZeroLag(this.groupId)) < maxTime
          || maxLag > this.configReader.getSmallDeltaValue()) {
        try {
          Thread.sleep(this.configReader.getSleepTimeMs());
          System.out.println("topic--" + rec.topic() + "-" + rec.partition() + "-" + rec.offset() + "--diff to catch--"
              + (recTimestamp - maxTime));
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
