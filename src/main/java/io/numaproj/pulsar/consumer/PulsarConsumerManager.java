package io.numaproj.pulsar.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import io.numaproj.pulsar.config.consumer.PulsarConsumerProperties;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.schema.GenericRecord;

/**
 * PulsarConsumerManager creates and maintains a single Consumer instance.
 * A new consumer is created based on the provided batch size and read timeout,
 * but once created it will be reused until explicitly removed.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "spring.pulsar.consumer", name = "enabled", havingValue = "true")
public class PulsarConsumerManager {

    @Autowired
    private PulsarConsumerProperties pulsarConsumerProperties;

    @Autowired
    private PulsarClient pulsarClient;

    // The current consumer instance.
    private Consumer<GenericRecord> currentConsumer;

    // Returns the current consumer if it exists. If not, creates a new one.
    public Consumer<GenericRecord> getOrCreateConsumer(long count, long timeoutMillis)
            throws PulsarClientException {
        if (currentConsumer != null) {
            return currentConsumer;
        }

        BatchReceivePolicy batchPolicy = BatchReceivePolicy.builder()
                .maxNumMessages((int) count)
                .timeout((int) timeoutMillis, TimeUnit.MILLISECONDS) // We do not expect user to specify a number larger
                                                                     // than 2^63 - 1 which will cause an overflow
                .build();

        currentConsumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .loadConf(pulsarConsumerProperties.getConsumerConfig())
                .batchReceivePolicy(batchPolicy)
                .subscriptionType(SubscriptionType.Shared) // Must be shared to support multiple pods
                .subscribe();

        log.info("Created new consumer with batch receive policy of: {} and timeoutMillis: {}", count, timeoutMillis);
        return currentConsumer;
    }

    @PreDestroy
    public void cleanup() {
        if (currentConsumer != null) {
            try {
                currentConsumer.close();
                log.info("Consumer closed during cleanup.");
            } catch (PulsarClientException e) {
                log.error("Error while closing consumer in cleanup", e);
            }
        }

        if (pulsarClient != null) {
            try {
                pulsarClient.close();
                log.info("Pulsar client closed during cleanup.");
            } catch (PulsarClientException e) {
                log.error("Error while closing the Pulsar client in cleanup", e);
            }
        }
    }
}