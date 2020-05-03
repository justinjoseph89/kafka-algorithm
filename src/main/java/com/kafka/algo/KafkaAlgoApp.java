package com.kafka.algo;

import com.kafka.algo.runners.KafkaAlgoAppRunner;

public class KafkaAlgoApp {

	public static void main(String[] args) {

		KafkaAlgoAppRunner runner = new KafkaAlgoAppRunner("input-topic-1", "output-topic-1", "13");
		runner.start();

	}

}
