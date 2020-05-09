package com.kafka.algo;

import com.kafka.algo.runner.configreader.KafkaConfigReader;
import com.kafka.algo.runners.KafkaAlgoAppRunner;

public class KafkaAlgoApp {

	public static void main(String[] args) {

		KafkaConfigReader configReader = new KafkaConfigReader();
		String inputTopicName = configReader.getTopics().get("input-topic-1");
		String outputTopicName = configReader.getTopics().get("output-topic-1");
		KafkaAlgoAppRunner runner = new KafkaAlgoAppRunner(inputTopicName, outputTopicName, configReader);
		runner.start();

	}

}
