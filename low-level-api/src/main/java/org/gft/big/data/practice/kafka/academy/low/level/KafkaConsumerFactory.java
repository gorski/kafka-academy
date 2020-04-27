package org.gft.big.data.practice.kafka.academy.low.level;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerFactory {

    private String bootstrapServers;
    private String consumerGroup;

    KafkaConsumerFactory(String bootstrapServers, String consumerGroup) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroup = consumerGroup;
    }

    public KafkaConsumer<Long, String> get() {
        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        return new KafkaConsumer<>(consumerConfigs);
    }


}
