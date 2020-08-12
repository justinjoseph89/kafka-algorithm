package com.kafka.algo;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSimpleProducer {
  private static final String CUSTOMER_SCHEMA = "{\"namespace\": \"customer.avro\", \"type\": \"record\", "
      + "\"name\": \"customer_details\"," + "\"fields\": [" + "{\"name\": \"customer_id\", \"type\": \"string\"},"
      + "{\"name\": \"dep_id\", \"type\": \"string\"},{\"name\": \"customer_name\", \"type\": \"string\"},{\"name\": \"insert_dt\", \"type\": \"string\"}"
      + "]}";
  private static final String DEPARTMENT_SCHEMA = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
      + "\"name\": \"dep_details\"," + "\"fields\": [" + "{\"name\": \"dep_id\", \"type\": \"string\"},"
      + "{\"name\": \"dep_name\", \"type\": \"string\"},{\"name\": \"upd_dt\", \"type\": \"string\"}" + "]}";

  public static void main(String[] args) throws InterruptedException {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    props.put("schema.registry.url", "http://localhost:8081/");

    KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

    final Schema.Parser parser = new Schema.Parser();
    final Schema customerSchema = parser.parse(CUSTOMER_SCHEMA);

    final Schema.Parser parser2 = new Schema.Parser();
    final Schema depSchema = parser2.parse(DEPARTMENT_SCHEMA);

//    int j = 0;
//    for (int i = 0; i < 1000; i++) {
//      j++;
//      System.out.println(i);
//
//      GenericRecord customerRec = new GenericData.Record(customerSchema);
//      customerRec.put("customer_id", String.valueOf(i));
//      customerRec.put("dep_id", String.valueOf(i));
//      customerRec.put("customer_name", "Customer-" + String.valueOf(i));
//      customerRec.put("insert_dt", String.valueOf(new Timestamp(System.currentTimeMillis())));
//
//      ProducerRecord<String, GenericRecord> producerRecr = new ProducerRecord<String, GenericRecord>("input-topic-2",
//          i + "", customerRec);
//      producer.send(producerRecr);
//      Thread.sleep(50);
//      if (j == 10) {
//        GenericRecord depRec = new GenericData.Record(depSchema);
//        depRec.put("dep_id", String.valueOf(i));
//        depRec.put("dep_name", "Dep-" + String.valueOf(i));
//        depRec.put("upd_dt", String.valueOf(new Timestamp(System.currentTimeMillis())));
//        ProducerRecord<String, GenericRecord> producerRecr1 = new ProducerRecord<String, GenericRecord>("input-topic-1",
//            i + "", depRec);
//        producer.send(producerRecr1);
//        Thread.sleep(10);
//        j = 0;
//      }
//    }
//    
//    Thread.sleep(60000);

    int k = 0;
    for (int m = 0; m < 100; m++) {
      k++;
      System.out.println(m);

      GenericRecord customerRec1 = new GenericData.Record(customerSchema);
      customerRec1.put("customer_id", String.valueOf(m));
      customerRec1.put("dep_id", String.valueOf(m));
      customerRec1.put("customer_name", "Customer-" + String.valueOf(m));
      customerRec1.put("insert_dt", String.valueOf(new Timestamp(System.currentTimeMillis())));

      ProducerRecord<String, GenericRecord> producerRecr4 = new ProducerRecord<String, GenericRecord>("input-topic-2",
          m + "", customerRec1);
      producer.send(producerRecr4);
      Thread.sleep(50);
      if (k == 10) {
        GenericRecord depRec1 = new GenericData.Record(depSchema);
        depRec1.put("dep_id", String.valueOf(m));
        depRec1.put("dep_name", "Dep-" + String.valueOf(m));
        depRec1.put("upd_dt", String.valueOf(new Timestamp(System.currentTimeMillis())));
        ProducerRecord<String, GenericRecord> producerRecr3 = new ProducerRecord<String, GenericRecord>("input-topic-1",
            m + "", depRec1);
        producer.send(producerRecr3);
        Thread.sleep(10);
        k = 0;
      }

    }

    producer.close();
  }

}