package com.kafka.algo.runners;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.time.Duration;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

	public KafkaAlgoAppRunner(final String inputTopicName, final KafkaConfigReader configReader,
			final String inputTopicNameList) {
		this.inputTopicName = inputTopicName;
		// this.outputTopicName = inputTopicName + "-temp";
		this.outputTopicName = inputTopicName;
		this.configReader = configReader;
		this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion();
		this.fieldSelector = new FieldSelector();
		this.outputTopicNameList = inputTopicNameList;
		// this.outputTopicNameList = "input-topic-1-temp,input-topic-2-temp";
		this.kafkaUtils = new KafkaUtils<K, V>(this.inputTopicName, this.configReader);
		this.isZkNodeUpd = this.configReader.isZkNodeUpd();
	}

	public void start() {
		final ZkConnect zk = new ZkConnect(this.inputTopicName, isZkNodeUpd, this.configReader);
		final ConsumerGroupTargetLag<K, V> targetLag = new ConsumerGroupTargetLag<K, V>(this.inputTopicName,
				this.configReader, this.outputTopicNameList);

		final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
				KafkaConnection.getKafkaConsumerProperties(this.groupId, this.configReader));

		final ConsumerGroupSourceLag<K, V> consumerLag = new ConsumerGroupSourceLag<K, V>(consumer, this.configReader);
		final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
				KafkaConnection.getKafkaProducerProperties(this.configReader));
		consumer.subscribe(java.util.Arrays.asList(this.inputTopicName));

		final String fieldNameToConsider = this.configReader.getTopicFields().get(this.inputTopicName);

		boolean considerLag = false;

		while (true) {
			//Need to configure the polling interval
			ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(10000));
			for (ConsumerRecord<K, V> rec : records) {
				messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);
			}
			considerLag = true;

		}
	}

	private void messageSendRecursive(final ConsumerRecord<K, V> rec, final KafkaProducer<K, V> producer,
			final ConsumerGroupSourceLag<K, V> consumerLag, final boolean considerLag, final ZkConnect zk,
			final String fieldNameToConsider, final ConsumerGroupTargetLag<K, V> targetLag) {

		final long recTimestamp = this.fieldSelector.getTimestampFromData(fieldNameToConsider, rec);
		final long lag = consumerLag.getConsumerGroupLag(this.groupId, this.inputTopicName, rec);
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
			}
		}

		if (this.leadTime > this.configReader.getSmallDeltaValue()) {
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
			messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);

		}

	}

}
