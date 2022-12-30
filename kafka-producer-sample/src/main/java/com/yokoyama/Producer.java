package com.yokoyama;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Producer {
    public static void main(String[] args) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        String topic = "ads-log";
        int campaignNum = 3;
        for (int i = 0; i < campaignNum * 5; i++) {
            int campaignId = i % campaignNum;
            String ots = UUID.randomUUID().toString();
            String logType;
            if (Math.random() < 0.3) {
                logType = "click";
            } else {
                logType = "vimp";
            }
            String event = String.format("logtype=%s,campaign_id=%s,timestamp=%s", logType, campaignId, System.currentTimeMillis() / 1000);
            producer.send(new ProducerRecord<>(topic, String.valueOf(campaignId), event));
            System.out.println(event);
        }
        producer.close();
    }
}
