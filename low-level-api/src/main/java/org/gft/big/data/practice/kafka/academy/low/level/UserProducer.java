package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikhalskiy.futurity.Futurity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.gft.big.data.practice.kafka.academy.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserProducer class and
 * implement the produceUsers method.
 * <p>
 * To serialize each User to JSON (use the ObjectMapper to achieve this),
 * Send it to Kafka under the user's id and return a single CompletableFuture which finishes
 * if / when all the user records are sent to Kafka,
 * <p>
 * Use the Futurity.shift method to transform a simple Java Future to a CompletableFuture
 * Once implemented, run the UserProducerTest to check the correctness of your implementation.
 */
public class UserProducer {

    private final static Logger log = LoggerFactory.getLogger(UserConsumer.class);

    private ObjectMapper objectMapper;
    private KafkaProducer<Long, String> producer;

    public UserProducer(ObjectMapper objectMapper, String bootstrapServers) {
        this.objectMapper = objectMapper;
        this.producer = new KafkaProducerFactory(bootstrapServers).get();
    }

    public CompletableFuture<?> produceUsers(String topic, Collection<User> users) {
        return users.stream()
                .map(user -> {
                    try {
                        return new ProducerRecord<>(topic, user.getId(), objectMapper.writeValueAsString(user));
                    } catch (JsonProcessingException e) {
                        log.error("Error: {}", e.getMessage());
                        return null;
                    }
                })
                .map(producer::send)
                .map(Futurity::shift)
                .reduce((left, right) -> left.thenCompose(any -> right))
                .get();
    }
}
