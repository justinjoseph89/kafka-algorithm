package com.kafka.algo;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.kafka.algo.runners.KafkaAlgoAppRunner;
import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.exception.NotEnoughArgumentException;
import com.kafka.algo.runners.kafkautils.KafkaConnection;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;
import static com.kafka.algo.runners.constants.Constants.INPUT_TOPIC_LIST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class KafkaAlgoApp {
  private static final Logger LOGGER = Logger.getLogger(KafkaAlgoApp.class.getName());

  public static <K, V> void main(String[] args) throws NotEnoughArgumentException {

    if (args.length < 1) {
      LOGGER.error("Please provide YAML file path");
      throw new NotEnoughArgumentException("YAML file not found check path " + args[0]);
    }
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    final KafkaConfigReader configReader = new KafkaConfigReader(args[0]);

    final List<KafkaConsumer<K, V>> consumerList = new ArrayList<KafkaConsumer<K, V>>();

    final String inputTopicNameList = configReader.getTopics().get(INPUT_TOPIC_LIST);
    final long consumerThreads = configReader.getConsumerThreads();

    Arrays.asList(inputTopicNameList.split(",")).forEach(topicName -> {
      for (int i = 0; i < consumerThreads; i++) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
            KafkaConnection.getKafkaConsumerProperties(GROUPID_PREFIX + configReader.getAppVersion(), configReader));
        consumer.assign(Collections.singleton(new TopicPartition(topicName, i)));
        consumerList.add(consumer);
      }
    });

    consumerList.parallelStream().forEach(consumer -> {
      consumer.assignment().forEach(tp -> {
        final KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(tp.topic(), tp.partition(), configReader,
            inputTopicNameList, consumer);
        runner.start();
      });

    });

//    Arrays.asList(inputTopicNameList.split(",")).parallelStream().forEach(inputTopic -> {
//      LongStream.range(0, consumerThreads).parallel().forEach(threads -> {
//        final KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(inputTopic, configReader,
//            inputTopicNameList);
//        runner.start();
//      });
//    });

    try {
      shutdownLatch.await();
      LOGGER.info("Copy Process Shutdown complete");
    } catch (InterruptedException e) {
      LOGGER.warn("Shutdown of the copy processor thread interrupted");
    }

  }

}
