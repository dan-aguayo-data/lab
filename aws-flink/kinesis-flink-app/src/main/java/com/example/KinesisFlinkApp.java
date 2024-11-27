package com.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;

import java.util.Properties;

public class KinesisFlinkApp {
    public static void main(String[] args) throws Exception {
        // Setup execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up properties for Kinesis Consumer
        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty("aws.region", "ap-southeast-2"); // Your AWS region
        kinesisConsumerConfig.setProperty("flink.stream.initpos", "LATEST"); // Start position

        // Optional AWS credentials (if not using IAM roles or default provider chain)
        // kinesisConsumerConfig.setProperty("aws.accessKeyId", "your-access-key-id");
        // kinesisConsumerConfig.setProperty("aws.secretKey", "your-secret-access-key");

        // Add Kinesis source to Flink job
        env.addSource(new FlinkKinesisConsumer<>(
                "data-team-poc-kinesis",   // Your Kinesis stream name
                new SimpleStringSchema(),  // Deserialization schema
                kinesisConsumerConfig
        )).print(); // Replace this with your Sink logic

        // Execute Flink job
        env.execute("Kinesis Flink Stream App");
    }
}
