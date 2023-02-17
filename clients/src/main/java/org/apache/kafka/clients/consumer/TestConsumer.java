package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class TestConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.103.171:9092");

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("test", 0));
        partitions.add(new TopicPartition("test", 1));

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(prop);
        consumer.assign(partitions);

        while(true) {
            consumer.poll(Duration.ofSeconds(1));
        }
    }
}
