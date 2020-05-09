package com.kafka.algo.runners;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.algo.runner.configreader.KafkaConfigReader;
import com.kafka.algo.runners.utils.ConsumerGroupLag;
import com.kafka.algo.runners.utils.KafkaConnection;
import com.kafka.algo.runners.utils.KafkaUtils;
import com.kafka.algo.runners.utils.ZkConnect;

/**
 * @author justin
 *
 */
public class KafkaAlgoAppRunner {
	private long maxTime;
	private long leadTime = 0; // Initial Assignment
	private String inputTopicName;
	private String outputTopicName;
	private String groupId;
	private KafkaConfigReader configReader;

	public KafkaAlgoAppRunner(final String inputTopicName, final String outputTopicName,
			final KafkaConfigReader configReader) {
		this.inputTopicName = inputTopicName;
		this.outputTopicName = outputTopicName;
		this.configReader = configReader;
		this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion() + System.currentTimeMillis();
	}

	public void start() {

		final ZkConnect zk = new ZkConnect(this.inputTopicName, true, this.configReader);
		final KafkaUtils kafkaUtils = new KafkaUtils(this.inputTopicName, this.configReader);

		maxTime = kafkaUtils.getTopicMinimumTime();

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				KafkaConnection.getKafkaJsonConsumerProperties(this.groupId, this.configReader));

		final ConsumerGroupLag<String, String> consumerLag = new ConsumerGroupLag<String, String>(consumer,
				this.configReader);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(
				KafkaConnection.getKafkaSimpleProducerProperties(this.configReader));

		consumer.subscribe(java.util.Arrays.asList(this.inputTopicName));

		boolean considerLag = false;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

			for (ConsumerRecord<String, String> rec : records) {
				messageSendRecursive(rec, producer, consumerLag, considerLag, zk);
			}
			considerLag = true;

		}
	}

	private void messageSendRecursive(final ConsumerRecord<String, String> rec,
			final KafkaProducer<String, String> producer, final ConsumerGroupLag<String, String> consumerLag,
			final boolean considerLag, final ZkConnect zk) {

		final long recTimestamp = rec.timestamp();

		final long lag = consumerLag.getConsumerGroupLag(this.groupId);
		// long maxLag = consumerLag.getMaxConsumerGroupLag();

		if (leadTime <= this.configReader.getSmallDeltaValue()) {
			// consider Lag should be 0 in order to consider leadtime as 0.
			// since there wont be any lag in the initial consumption
			if (lag == 0 && considerLag == true) {
				leadTime = 0;
			} else {
				leadTime = recTimestamp - maxTime;
			}

			if (leadTime <= this.configReader.getSmallDeltaValue()) {
				producer.send(new ProducerRecord<String, String>(outputTopicName, rec.key(), rec.value()));
			}
		}

		if (leadTime > this.configReader.getSmallDeltaValue()) {

			zk.updateNode(maxTime, rec.partition());

			// here need to think about the implementation of maxLag
			while (zk.getMinimum() < maxTime) {
				System.out.println(" going to sleep for 10 sec :" + this.inputTopicName + " -minTime-" + zk.getMinimum()
						+ " -maxTime- " + maxTime);
				// System.out.println(" going to sleep for 10 sec :" + zk.getMinimum() + "
				// -maxTime- " + maxTime
				// + " -maxLag- " + maxLag);
				try {
					Thread.sleep(this.configReader.getSleepTimeMs());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			maxTime = maxTime + this.configReader.getDeltaValue();
			leadTime = 0;
			messageSendRecursive(rec, producer, consumerLag, considerLag, zk);

		}

	}

}
