package com.example.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);

        createKafkaProducer();

        createKafkaConsumer();

    }

    private static void createKafkaConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = null;

        try {
            // Create the consumer
            consumer = new KafkaConsumer<>(props);

            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList("Messages"));

            // Continuously poll for new messages
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    }
                } catch (Exception e) {
                    System.err.println("Error during poll: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error creating or using consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the consumer
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error closing consumer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }


    private static void createKafkaProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        Producer<String, String> producer = null;

        try {
            // Create the producer
            producer = new KafkaProducer<>(props);

            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("Messages", "key", "Hello, Kafka!");

            // Send the record with a callback to handle exceptions
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        System.out.println("Record sent successfully to topic " + metadata.topic() + " partition " + metadata.partition() + " at offset " + metadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("Error creating or sending producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the producer
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.println("Error closing producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }


}
