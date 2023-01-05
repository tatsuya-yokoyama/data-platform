package com.yokoyama;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.clickhouse.jdbc.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.*;
import java.util.Properties;

public class FlinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:29092")
                .setTopics("ads-log")
                .setGroupId("group1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.addSink(new SinkFunction<>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                String[] events = value.split(",");
                String logType = events[0].split("=")[1];
                String campaignId = events[1].split("=")[1];
                String timestamp = events[2].split("=")[1];
                String table;
                if (logType.equals("vimp")) {
                    table = "training.action_vimp";
                } else {
                    table = "training.action_click";
                }
                String query = String.format("INSERT INTO %s (campaign_id, user_id, timestamp) VALUES (%s,1,%s);", table, campaignId, timestamp);

                String url = "jdbc:ch://localhost:18123";
                Properties properties = new Properties();
                ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
                try {
                    Connection connection = dataSource.getConnection("default", null);
                    Statement statement = connection.createStatement();
                    statement.executeQuery(query);
                } catch (Exception e) {
                    System.out.println(e.toString());
                } finally {
                    System.out.println(query);
                }

            }
        });
        env.execute();
    }
}
