package com.kafka.algo;

import static com.kafka.algo.runners.constants.Constants.GROUPID_PREFIX;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import com.kafka.algo.runners.kafkautils.KafkaConnection;

public class KafkaSimpleCosumer {

  public static void main(String[] args) throws InterruptedException {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9095");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", "http://localhost:8081/");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "SOME-TEST9");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    String inputTopicNameList = "input-topic-1,input-topic-2";
    final List<KafkaConsumer<String, GenericRecord>> consumerList = new ArrayList<KafkaConsumer<String, GenericRecord>>();
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < inputTopicNameList.split(",").length; j++) {
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(inputTopicNameList.split(",")[j]));
        System.out.println(consumer);
        consumerList.add(consumer);
      }
    }

    System.out.println(consumerList.size());

    consumerList.parallelStream().forEach(consumer -> {
      methods(consumer);
    });

  }

  public static void methods(KafkaConsumer<String, GenericRecord> consumer) {
    System.out.println(consumer.subscription().stream().findFirst().get());
    while (true) {
      final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, GenericRecord> rec : records) {
        System.out.println(rec.partition() + "-" + rec.offset());
      }

    }

  }

}