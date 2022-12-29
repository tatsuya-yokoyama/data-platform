package com.yokoyama;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    public static void main(String[] args) {

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        String topic = "ads-log";
        consumer.subscribe(List.of(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));
            records.forEach(record -> System.out.println(record.value()));
        }
    }
}
