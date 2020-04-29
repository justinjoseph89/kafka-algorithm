package com.kafka.consumers.utils;

import java.time.Duration;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TimeSyncLatest {

	private AtomicLong maxTimestamp = new AtomicLong();
	private long maxTime;

	public TimeSyncLatest(long maxTime) {
		this.maxTime = maxTime;
	}

	public long getMaximumTime() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
				KafkaUtilities.getKafkaJsonConsumerProperties(System.currentTimeMillis()));
		consumer.subscribe(Collections.singletonList("input_topic_1"));
		consumer.poll(Duration.ofMillis(1000));

		consumer.beginningOffsets(consumer.assignment()).forEach((topicPartition, offset) -> {
			System.out.println(topicPartition + "---" + offset);
			consumer.seek(topicPartition, offset);
			consumer.poll(Duration.ofMillis(100)).forEach(record -> {
				System.out.println(record.value());

			});
		});
		return maxTimestamp.get();
	}

	// This we need to get as lazy evaluation using spring annotation
	public long getMinimumTime() {
		TreeSet<Long> set = new TreeSet<>();
		try {
			KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(
					KafkaUtilities.getKafkaJsonConsumerPropertiesLong(System.currentTimeMillis()));
			consumer.subscribe(Collections.singletonList("time_sync"));
			consumer.poll(Duration.ofMillis(100)).forEach(record -> {
				set.add(record.value());
			});

		} catch (Exception e) {
			KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(
					KafkaUtilities.getKafkaJsonConsumerPropertiesLong(System.currentTimeMillis()));
			consumer.subscribe(Collections.singletonList("time_sync"));
			consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
				set.add(record.value());
			});
			return set.size() > 0 ? set.first() : maxTime;
		}
		return set.size() > 0 ? set.first() : maxTime;

	}

	public static void main(String[] args) {
		TimeSyncLatest sync = new TimeSyncLatest(1111111111111L);
		System.out.println(sync.getMinimumTime());
	}

}
