package io.numaproj.pulsar.config.consumer;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
public class PulsarConsumerConfig {

    @Bean
    @ConditionalOnProperty(prefix = "spring.pulsar.consumer", name = "enabled", havingValue = "true", matchIfMissing = false)
    public Consumer<byte[]> pulsarConsumer(PulsarClient pulsarClient, PulsarConsumerProperties pulsarConsumerProperties)
            throws Exception {

        BatchReceivePolicy batchPolicy = BatchReceivePolicy.builder()
                .maxNumMessages(3000)
                .timeout(30, TimeUnit.SECONDS) 
                .build();

        Consumer<byte[]> currentConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .loadConf(pulsarConsumerProperties.getConsumerConfig())
                .batchReceivePolicy(batchPolicy)
                .subscriptionType(SubscriptionType.Shared) // Must be shared to support multiple pods
                .subscribe();

        log.info("Created new consumer with batch receive policy of: {} and timeoutMillis: {}", 3000, "30 sec");
        return currentConsumer;
    }
}