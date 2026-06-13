package com.example;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithConsumerGroups {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithConsumerGroups.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-Java-application";
        String topic = "demo_java_new";

        // Steps to create a Kafka Consumer
        // 1. Create Consumer Properties
        Properties properties = new Properties();
        // connect to local Kafka cluster
        properties.setProperty("bootstrap.servers", "172.17.110.18:9092");
        // change the above IP address value to your local Kafka cluster's IP address

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // set the consumer group id
        properties.setProperty("group.id", groupId);

        // none - throw an error if no previous offset is found for the consumer group
        // earliest - read from the beginning of the topic
        // latest - read from the end of the topic
        properties.setProperty("auto.offset.reset", "earliest"); // earliest, latest, none

        // 2. Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

            // poll for new data
            while (true) {
                // log.info("Polling for data...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {} " + record.key() + ", Value: {} " + record.value());
                    log.info("Partition: {} " + record.partition() + ", Offset: {} " + record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            log.error("Unexpected error", e);
        } finally {
            consumer.close(); // close the consumer to commit the final offsets and leave the group cleanly
            log.info("The consumer is now gracefully Shutdown.");
        }

    }

}


//run three instance of this consumer and see how the messages are distributed among the three instances of the consumer group.