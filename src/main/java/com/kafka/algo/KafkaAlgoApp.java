package com.kafka.algo;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.kafka.algo.runners.KafkaAlgoAppRunner;
import com.kafka.algo.runners.configreader.KafkaConfigReader;
import com.kafka.algo.runners.exception.NotEnoughArgumentException;
import com.kafka.algo.runners.kafkautils.KafkaConnection;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;
import static com.kafka.algo.runners.constants.Constants.INPUT_TOPIC_LIST;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class KafkaAlgoApp {
  private static final Logger LOGGER = Logger.getLogger(KafkaAlgoApp.class.getName());

  public static <K, V> void main(String[] args) throws NotEnoughArgumentException {

    if (args.length < 1) {
      LOGGER.error("Please provide YAML file path");
      throw new NotEnoughArgumentException("YAML file not found check path " + args[0]);
    }
    System.out.println(args[0]);

    final KafkaConfigReader configReader = new KafkaConfigReader(args[0]);

    final String inputTopicNameList = configReader.getTopics().get(INPUT_TOPIC_LIST);
    final long consumerThreads = configReader.getConsumerThreads();

    final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
        KafkaConnection.getKafkaProducerProperties(configReader));

    Properties targetProperties = new Properties();
    targetProperties.put("bootstrap.servers", configReader.getBootstrapTargetServers());

    Properties sourceProps = new Properties();
    sourceProps.put("bootstrap.servers", configReader.getBootstrapServers());

    AdminClient targetClient = AdminClient.create(targetProperties);
    AdminClient sourceClient = AdminClient.create(sourceProps);

    Properties consumerProps = KafkaConnection.getKafkaConsumerProperties(GROUPID_PREFIX + configReader.getAppVersion(),
        configReader);

    KafkaConsumer<K, V> targetKafkaConsuemr = new KafkaConsumer<>(
        KafkaConnection.getKafkaTargetConsumerProperties(configReader));
    ZooKeeper zk = connectZookeeper(configReader.getZookeeperHost());

    CountDownLatch shutdownLatch = new CountDownLatch(8);
    Arrays.asList(inputTopicNameList.split(",")).parallelStream().forEach(topicName -> {
      KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps);
      consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          System.out.printf("onPartitionsRevoked - consumerName: %s,partitions: %s%n", consumer.subscription(),
              formatPartitions(partitions));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          System.out.printf("onPartitionsAssigned - consumerName: %s, partitions: %s%n", consumer.subscription(),
              formatPartitions(partitions));
        }
      });

      final KafkaAlgoAppRunner<K, V> runner = new KafkaAlgoAppRunner<K, V>(topicName, configReader, inputTopicNameList,
          consumer, producer, targetClient, sourceClient, targetKafkaConsuemr, zk);
      runner.start();
    });

    try {
      shutdownLatch.await();
      LOGGER.info("Copy Process Shutdown complete");
    } catch (InterruptedException e) {
      LOGGER.warn("Shutdown of the copy processor thread interrupted");
    }

  }

  private static List<String> formatPartitions(Collection<TopicPartition> partitions) {
    return partitions.stream().map(
        topicPartition -> String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
        .collect(Collectors.toList());
  }

  /**
   * Create Zookeeper client object to get apis
   * 
   * @param host
   * @return
   */
  private static ZooKeeper connectZookeeper(final String host) {
    CountDownLatch connSignal = new CountDownLatch(0);
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(host, 3000, new Watcher() {
        public void process(WatchedEvent event) {
          if (event.getState() == KeeperState.SyncConnected) {
            connSignal.countDown();
          }
        }
      });
      connSignal.await();
    } catch (IOException | InterruptedException e) {
      LOGGER.error("Exception while getting connection of the node.. " + e.getMessage());
    }
    return zk;
  }

}
