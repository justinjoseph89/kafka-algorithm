package com.kafka.consumers;

import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.consumers.utils.KafkaUtilities;
import com.kafka.consumers.utils.TimeSyncLatest;

public class KStreamConsumerProducer {
	private static long delta = 200L;
	private static long smallDelta = 10;
	private static long maxTime = 1587994312685L; // Need to find this automatically, currently in hardcoded way
	private static long leadTime = 0; // Initial Assignment

	public static void main(String[] args) throws InterruptedException, IOException {
		// This should be replaced by the internal apis of context.forward for better
		// performance
		TimeSyncLatest syncUtil = new TimeSyncLatest(maxTime);

		// This should be replaced by the internal apis of context.forward for better
		// performance
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				KafkaUtilities.getKafkaJsonConsumerProperties(System.currentTimeMillis()));
		KafkaProducer<String, Long> sync_producer = new KafkaProducer<>(
				KafkaUtilities.getKafkaSimpleProducerPropertiesLong());
		KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaUtilities.getKafkaSimpleProducerProperties());

		consumer.subscribe(java.util.Arrays.asList("input_topic_1"));

		while (true) {
			System.out.println("Operation Start Time--1--" + System.currentTimeMillis());
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

			for (ConsumerRecord<String, String> rec : records) {
				messageSendRecursive(rec, sync_producer, producer, syncUtil);
				// Thread.sleep(1000);
			}
			System.out.println("Operation End Time-1---" + System.currentTimeMillis());

		}
	}

	private static void messageSendRecursive(ConsumerRecord<String, String> rec,
			KafkaProducer<String, Long> sync_producer, KafkaProducer<String, String> producer, TimeSyncLatest syncUtil)
			throws InterruptedException {

		if (leadTime <= smallDelta) {
			leadTime = rec.timestamp() - maxTime;

			if (leadTime <= smallDelta) {
				producer.send(new ProducerRecord<String, String>("output_topic_1", rec.key(), rec.value()));
			}
		}

		if (leadTime > smallDelta) {
			sync_producer.send(new ProducerRecord<String, Long>("time_sync", "input_topic_1", maxTime));
			System.out.println(" message send to time_sync");

			while (syncUtil.getMinimumTime() < maxTime) {
				System.out.println(" going to sleep for 10 sec :" + syncUtil.getMinimumTime() + " -- " + maxTime);
				Thread.sleep(1000);

			}
			maxTime = maxTime + delta;
			leadTime = 0;
			messageSendRecursive(rec, sync_producer, producer, syncUtil);
		}

	}

}
