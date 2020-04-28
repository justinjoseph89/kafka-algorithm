package com.kafka.consumers;

import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KStreamConsumerProducer {
	private static long delta = 100;
	private static long smallDelta = 1;
	private static long maxTime = 1587994312685L; // Need to find this automatically, currently in hardcoded way

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
			System.out.println("Operation Start Time--1--"+System.currentTimeMillis());
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

			for (ConsumerRecord<String, String> rec : records) {
				messageSendRecursive(rec, sync_producer, producer, syncUtil);
//				System.out.println("going to exit for while to bring more....");
			}
			System.out.println("Operation End Time-1---"+System.currentTimeMillis());

		}
	}

	private static void messageSendRecursive(ConsumerRecord<String, String> rec,
			KafkaProducer<String, Long> sync_producer, KafkaProducer<String, String> producer, TimeSyncLatest syncUtil)
			throws InterruptedException {
//		System.out.println("maxTime------" + maxTime);
		long leadTime = 0; // Initial Assignment
		if (leadTime < smallDelta) {
//			System.out.println("     in first if condition------");
//			System.out.println("     rec.timestamp()------" + rec.timestamp());
//			System.out.println("     maxTime--------------" + maxTime);
			leadTime = rec.timestamp() - maxTime;
//			System.out.println("     new leadTime--------- " + leadTime);

			if (leadTime <= smallDelta) {
				producer.send(new ProducerRecord<String, String>("output_topic_1", rec.key(), rec.value()));
//				System.out.println("     message send from topic 1");
				maxTime = maxTime + delta;
			}
		}

		if (leadTime > smallDelta) {
//			System.out.println("     in second if condition------");
			sync_producer.send(new ProducerRecord<String, Long>("time_sync", "input_topic_1", maxTime));
//			System.out.println("     message send to time_sync");
			long syncMinTime = syncUtil.getMinimumTime();
//			System.out.println("     getMinimumTime-------" + syncMinTime);
			if (syncMinTime < maxTime) {
//				System.out.println("          going to sleep for 10 sec");
				Thread.sleep(10000);
				maxTime = maxTime + delta;
				System.out.println("          maxTime after sleeping---" + maxTime);
//				System.out.println("          maxTime in last condition resending message again---");
				messageSendRecursive(rec, sync_producer, producer, syncUtil);
			} else {
//				System.out.println("          else am not going to sleep");
				maxTime = maxTime + delta;
//				System.out.println("          else maxTime after sleeping---" + maxTime);
				messageSendRecursive(rec, sync_producer, producer, syncUtil);
			}
		}

	}

}
