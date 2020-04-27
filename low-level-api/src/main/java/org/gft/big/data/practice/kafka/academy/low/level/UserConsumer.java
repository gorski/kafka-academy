package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserConsumer class and implement
 * the consumeUsers method so that it continuously reads the records from a given Kafka topic
 * until the maxRecords record count or the timeout is reached and then deserializes each value
 * to a User class from JSON (use the ObjectMapper) to eventually return the retrieved users.
 * <p>
 * One implemented, run the UserConsumerTest to check the correctness of your implementation
 */
public class UserConsumer {

    private final static Logger log = LoggerFactory.getLogger(UserConsumer.class);

    private ObjectMapper mapper;

    private Duration pollingTimeout;
    private KafkaConsumer<Long, String> consumer;

    public UserConsumer(ObjectMapper mapper, Duration pollingTimeout, String bootstrapServers, String consumerGroup) {
        this.mapper = mapper;
        this.pollingTimeout = pollingTimeout;
        this.consumer = new KafkaConsumerFactory(bootstrapServers, consumerGroup).get();
    }

    public List<User> consumeUsers(String topic, Duration timeout, long maxRecords) {

        consumer.subscribe(Collections.singletonList(topic));

        List<User> users = new LinkedList<>();
        LocalDateTime startTimestamp = LocalDateTime.now();
        while (Duration.between(startTimestamp, LocalDateTime.now()).compareTo(timeout) <= 0 && users.size() <= maxRecords) {
            consumer.poll(pollingTimeout).records(topic).forEach(record -> {
                try {
                    users.add(mapper.readValue(record.value(), User.class));
                } catch (IOException e) {
                    log.error("Error: {}", e.getMessage());
                }
            });
        }
        return users;
    }
}
