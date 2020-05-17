package com.kafka.algo.runners;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.time.Duration;
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
	private long maxTime;
	private long leadTime = 0; // Initial Assignment
	private String inputTopicName;
	private String outputTopicName;
	private String groupId;
	private KafkaConfigReader configReader;
	private FieldSelector fieldSelector;
	private String outputTopicNameList;

	public KafkaAlgoAppRunner(final String inputTopicName, final KafkaConfigReader configReader,
			final String inputTopicNameList) {
		this.inputTopicName = inputTopicName;
		this.outputTopicName = inputTopicName;
		this.configReader = configReader;
		this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion();
		this.fieldSelector = new FieldSelector();
		this.outputTopicNameList = inputTopicNameList;
	}

	public void start() {
		System.out.println(this.inputTopicName);
		final ZkConnect zk = new ZkConnect(this.inputTopicName, true, this.configReader);
		final KafkaUtils<K, V> kafkaUtils = new KafkaUtils<K, V>(this.inputTopicName, this.configReader);
		final ConsumerGroupTargetLag<K, V> targetLag = new ConsumerGroupTargetLag<K, V>(this.inputTopicName,
				this.configReader, this.outputTopicNameList);

		maxTime = kafkaUtils.getTopicMinimumTime();

		final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
				KafkaConnection.getKafkaConsumerProperties(this.groupId, this.configReader));

		final ConsumerGroupSourceLag<K, V> consumerLag = new ConsumerGroupSourceLag<K, V>(consumer, this.configReader);
		final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
				KafkaConnection.getKafkaProducerProperties(this.configReader));
		consumer.subscribe(java.util.Arrays.asList(this.inputTopicName));

		final String fieldNameToConsider = this.configReader.getTopicFields().get(this.inputTopicName);

		boolean considerLag = false;

		while (true) {
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

		final long lag = consumerLag.getConsumerGroupLag(this.groupId);
		final long maxLag = targetLag.getTargetActiveConsumerLag();

		if (leadTime <= this.configReader.getSmallDeltaValue()) {
			// consider Lag should be 0 in order to consider leadtime as 0.
			// since there wont be any lag in the initial consumption
			if (lag == 0 && considerLag == true) {
				leadTime = 0;
			} else {
				leadTime = recTimestamp - maxTime;
			}

			if (leadTime <= this.configReader.getSmallDeltaValue()) {
				producer.send(new ProducerRecord<K, V>(this.outputTopicName, rec.key(), rec.value()));
			}
		}

		if (leadTime > this.configReader.getSmallDeltaValue()) {

			zk.updateNode(maxTime, rec.partition());
			// here need to think about the implementation of maxLag
			while (zk.getMinimum() < maxTime || maxLag > this.configReader.getSmallDeltaValue()) {
				try {
					Thread.sleep(this.configReader.getSleepTimeMs());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			maxTime = maxTime + this.configReader.getDeltaValue();
			leadTime = 0;
			messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);

		}

	}

}
