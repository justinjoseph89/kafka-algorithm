package com.kafka.consumers;

import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.consumers.utils.ConsumerGroupLag;
import com.kafka.consumers.utils.KafkaConnection;
import com.kafka.consumers.utils.KafkaUtils;
import com.kafka.consumers.utils.ZkConnect;
import static com.kafka.consumers.utils.Constants.GROUPID_PREFIX;
import static com.kafka.consumers.utils.Constants.DELTA;
import static com.kafka.consumers.utils.Constants.SMALL_DELTA;

/**
 * @author justin
 *
 */
public class KStreamConsumerProducer {
	private static long maxTime;
	private static long leadTime = 0; // Initial Assignment

	public static void main(String[] args) throws InterruptedException, IOException {
		// This should be replaced by the internal apis of context.forward for better
		// performance
		ZkConnect zk = new ZkConnect("input-topic-1", true);
		KafkaUtils kafkaUtils = new KafkaUtils("input-topic-1");

		maxTime = kafkaUtils.getTopicMinimumTime();
		String groupId = GROUPID_PREFIX + System.currentTimeMillis();

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				KafkaConnection.getKafkaJsonConsumerProperties(groupId));

		ConsumerGroupLag<String, String> consumerLag = new ConsumerGroupLag<>(consumer);

		KafkaProducer<String, String> producer = new KafkaProducer<>(
				KafkaConnection.getKafkaSimpleProducerProperties());

		consumer.subscribe(java.util.Arrays.asList("input-topic-1"));

		boolean considerLag = false;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

			for (ConsumerRecord<String, String> rec : records) {
				messageSendRecursive(rec, producer, zk, consumerLag, groupId, considerLag);
			}
			considerLag = true;

		}
	}

	private static void messageSendRecursive(final ConsumerRecord<String, String> rec,
			final KafkaProducer<String, String> producer, final ZkConnect zk,
			final ConsumerGroupLag<String, String> consumerLag, final String groupId, final boolean considerLag)
			throws InterruptedException {

		long recTimestamp = rec.timestamp();

		long lag = consumerLag.getConsumerGroupLag(groupId);
		// long maxLag = consumerLag.getMaxConsumerGroupLag();

		if (leadTime <= SMALL_DELTA) {
			// consider Lag should be 0 in order to consider leadtime as 0.
			// since there wont be any lag in the initial consumption
			if (lag == 0 && considerLag == true) {
				leadTime = 0;
			} else {
				leadTime = recTimestamp - maxTime;
			}

			if (leadTime <= SMALL_DELTA) {
				producer.send(new ProducerRecord<String, String>("output-topic-1", rec.key(), rec.value()));
			}
		}

		if (leadTime > SMALL_DELTA) {

			zk.updateNode(maxTime, rec.partition());

			// here need to think about the implementation of maxLag
			while (zk.getMinimum() < maxTime) {
				System.out.println(" going to sleep for 10 sec :" + zk.getMinimum() + " -maxTime- " + maxTime);
				// System.out.println(" going to sleep for 10 sec :" + zk.getMinimum() + "
				// -maxTime- " + maxTime
				// + " -maxLag- " + maxLag);
				Thread.sleep(100);
			}
			maxTime = maxTime + DELTA;
			leadTime = 0;
			messageSendRecursive(rec, producer, zk, consumerLag, groupId, considerLag);

		}

	}

}
