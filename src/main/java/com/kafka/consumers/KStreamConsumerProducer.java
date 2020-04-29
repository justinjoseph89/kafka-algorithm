package com.kafka.consumers;

import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.consumers.utils.KafkaUtilities;
import com.kafka.consumers.utils.ZkConnect;

public class KStreamConsumerProducer {
	private static long delta = 200L;
	private static long smallDelta = 10;
	private static long maxTime = 1587994360116L; // Need to find this automatically
	private static long leadTime = 0; // Initial Assignment

	public static void main(String[] args) throws InterruptedException, IOException {

		// This should be replaced by the internal apis of context.forward for better
		// performance
		ZkConnect zk = new ZkConnect("input_topic_2", true);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				KafkaUtilities.getKafkaJsonConsumerProperties(System.currentTimeMillis()));
		consumer.subscribe(java.util.Arrays.asList("input_topic_2"));

		// This should be replaced by the internal apis of context.forward for better
		// performance
		KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaUtilities.getKafkaSimpleProducerProperties());

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
			for (ConsumerRecord<String, String> rec : records) {
				messageSendRecursive(rec, producer, zk);
			}

		}
	}

	/**
	 * @param rec
	 * @param producer
	 * @param zk
	 * @throws InterruptedException
	 */
	private static void messageSendRecursive(ConsumerRecord<String, String> rec, KafkaProducer<String, String> producer,
			ZkConnect zk) throws InterruptedException {

		if (leadTime <= smallDelta) {
			leadTime = rec.timestamp() - maxTime;
			if (leadTime <= smallDelta) {
				producer.send(new ProducerRecord<String, String>("output_topic_2", rec.key(), rec.value()));
			}
		}

		if (leadTime > smallDelta) {
			zk.updateNode(maxTime);
			System.out.println(" ---message send to time_sync");

			while (zk.getMinimum() < maxTime) {
				Thread.sleep(1000);
				System.out.println(" ---going to sleep for 10 sec :" + zk.getMinimum() + " -- " + maxTime);

			}
			maxTime = maxTime + delta;
			leadTime = 0;
			messageSendRecursive(rec, producer, zk);

		}

	}

}
