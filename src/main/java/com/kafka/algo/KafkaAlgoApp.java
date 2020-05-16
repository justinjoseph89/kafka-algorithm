package com.kafka.algo;

import org.apache.log4j.Logger;

import com.kafka.algo.runners.KafkaAlgoAppRunner;
import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.exception.NotEnoughArgumentException;
import static com.kafka.algo.runners.constants.Constants.INPUT_TOPIC_LIST;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class KafkaAlgoApp {
	private static final Logger LOGGER = Logger.getLogger(KafkaAlgoApp.class.getName());

	public static <K, V> void main(String[] args) throws NotEnoughArgumentException {

		if (args.length < 1) {
			LOGGER.error("YAML file not found. Provide path ");
			throw new NotEnoughArgumentException("YAML file not found. Provide path ");
		}
		CountDownLatch shutdownLatch = new CountDownLatch(1);
		final KafkaConfigReader configReader = new KafkaConfigReader(args[0]);

		final String inputTopicName = configReader.getTopics().get(INPUT_TOPIC_LIST);

		Arrays.asList(inputTopicName.split(",")).parallelStream().forEach(inputTopic -> {
			final KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(inputTopic, configReader);
			runner.start();
		});

		try {
			shutdownLatch.await();
			LOGGER.info("Copy Process Shutdown complete");
		} catch (InterruptedException e) {
			LOGGER.warn("Shutdown of the copy processor thread interrupted");
		}

	}

}
