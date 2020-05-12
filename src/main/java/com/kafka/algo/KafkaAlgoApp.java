package com.kafka.algo;

import com.kafka.algo.runners.KafkaAlgoAppRunner;
import com.kafka.algo.runners.configreader.KafkaConfigReader;

public class KafkaAlgoApp {

	public static <K, V> void main(String[] args) {

		KafkaConfigReader configReader = new KafkaConfigReader();
		String inputTopicName = configReader.getTopics().get("input-topic-1");
		String outputTopicName = configReader.getTopics().get("output-topic-1");
		KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(inputTopicName, outputTopicName, configReader);
		runner.start();

	}

}
