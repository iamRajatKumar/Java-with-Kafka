package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // Steps to create a Kafka Producer
        // 1. Create Producer Properties
        Properties properties = new Properties();
        // connect to local Kafka cluster
        properties.setProperty("bootstrap.servers", "172.17.110.18:9092"); 
        //change the above IP address value to your local Kafka cluster's IP address

        // set the key and value serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java_new", "Hello World ");

        // send data - asynchronous
        producer.send(producerRecord);

        // // send data - asynchronous with a callback
        // producer.send(producerRecord, (metadata, exception) -> {
        //     if (exception == null) {
        //         log.info("Message sent successfully!");
        //         log.info("Topic: {}", metadata.topic());
        //         log.info("Partition: {}", metadata.partition());
        //         log.info("Offset: {}", metadata.offset());
        //         log.info("Timestamp: {}", metadata.timestamp());
        //     } else {
        //         log.error("Error while producing", exception);
        //     }
        // });

        // flush data - synchronous
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // close the producer
        producer.close();

    }

}
