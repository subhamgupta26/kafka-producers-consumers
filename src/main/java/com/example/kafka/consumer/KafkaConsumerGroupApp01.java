package com.example.kafka.consumer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerGroupApp01 {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "test-group");

		KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(props);

		ArrayList<String> topics = new ArrayList<>();
		topics.add("my-big-topic");

		myConsumer.subscribe(topics);

		try {
			while (true) {
				ConsumerRecords<String, String> records = myConsumer.poll(10);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Topic: " + record.topic() + " Partition: " + record.partition() + " Offset: "
							+ record.offset() + " Key: " + record.key() + " Value: " + record.value().toUpperCase());
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		finally {
			myConsumer.close();
		}
	}
}
