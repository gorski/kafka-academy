package org.gft.big.data.practice.kafka.academy.low.level;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerFactory {

    private String bootstrapServers;

    KafkaProducerFactory(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public KafkaProducer<Long, String> get() {
        Map<String, Object> producerSettings = new HashMap<>();
        producerSettings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerSettings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerSettings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerSettings);
    }
}
