package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.integration.utils.KafkaEmbedded;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Walaa {
    final EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    final String zkHost = "127.0.0.1:";
    KafkaEmbedded broker;

    @Test
    public void haha() throws IOException {
        createBroker(zkHost+zookeeper.port());
        broker.createTopic("test",2,1);
        Utils.sleep(1000);
        Properties prop = new Properties();

        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(new TopicPartition("test", 0));
        partitions.add(new TopicPartition("test", 1));

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(prop);
        consumer.assign(partitions);
        System.out.println("gogobebe");
        while(true) {
            consumer.poll(Duration.ofSeconds(1));
        }


    }

    void createBroker(String zkAddr) throws IOException {
        Properties kafkaProp = new Properties();
        kafkaProp.put("listeners", "PLAINTEXT://:9092");
        kafkaProp.put("advertised.listeners", "PLAINTEXT://localhost:9092");
        kafkaProp.put("zookeeper.connect", zkAddr);
        broker = new KafkaEmbedded(kafkaProp, new MockTime());
    }
}
