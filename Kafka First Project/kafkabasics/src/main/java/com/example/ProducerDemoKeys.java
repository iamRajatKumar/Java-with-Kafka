package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // Steps to create a Kafka Producer
        // 1. Create Producer Properties
        Properties properties = new Properties();
        // connect to local Kafka cluster
        properties.setProperty("bootstrap.servers", "172.17.110.18:9092");
        // change the above IP address value to your local Kafka cluster's IP address

        // set the key and value serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 30; i++) {

                String topic = "demo_java_new";
                String key = "id_" + i; // keys are id_0, id_1, ..., id_29
                String value = "Hello Deepu !  " + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data - asynchronous with a callback
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("key: " + key + " | Partition: "+ metadata.partition());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // send data - asynchronous
        // producer.send(producerRecord);

        // flush data - synchronous
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // close the producer
        producer.close();

    }

}
