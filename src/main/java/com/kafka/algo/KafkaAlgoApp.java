package com.kafka.algo;

import org.apache.log4j.Logger;

import com.kafka.algo.runners.KafkaAlgoAppRunner;
import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.exception.NotEnoughArgumentException;
import static com.kafka.algo.runners.constants.Constants.INPUT_TOPIC_LIST;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

public class KafkaAlgoApp {
	private static final Logger LOGGER = Logger.getLogger(KafkaAlgoApp.class.getName());

	public static <K, V> void main(String[] args) throws NotEnoughArgumentException {

		if (args.length < 1) {
			LOGGER.error("Please provide YAML file path");
			throw new NotEnoughArgumentException("YAML file not found check path " + args[0]);
		}
		CountDownLatch shutdownLatch = new CountDownLatch(1);
		final KafkaConfigReader configReader = new KafkaConfigReader(args[0]);

		final String inputTopicNameList = configReader.getTopics().get(INPUT_TOPIC_LIST);
		final long consumerThreads = configReader.getConsumerThreads();
		Arrays.asList(inputTopicNameList.split(",")).parallelStream().forEach(inputTopic -> {
			LongStream.range(0, consumerThreads).parallel().forEach(threads -> {
				final KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(inputTopic, configReader,
						inputTopicNameList);
				runner.start();
			});
		});

		try {
			shutdownLatch.await();
			LOGGER.info("Copy Process Shutdown complete");
		} catch (InterruptedException e) {
			LOGGER.warn("Shutdown of the copy processor thread interrupted");
		}

	}

}
