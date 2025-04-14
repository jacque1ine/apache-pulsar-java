package io.numaproj.pulsar.config.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Configuration
public class PulsarProducerConfig {

    @Autowired
    private Environment env;

    @Bean
    @ConditionalOnProperty(prefix = "spring.pulsar.producer", name = "enabled", havingValue = "true", matchIfMissing = false)
    public Producer<byte[]> pulsarProducer(PulsarClient pulsarClient, PulsarProducerProperties pulsarProducerProperties)
            throws Exception {
        String podName = env.getProperty("NUMAFLOW_POD", "pod-" + UUID.randomUUID());
        String producerName = "producerName";

        Map<String, Object> producerConfig = pulsarProducerProperties.getProducerConfig();
        if (producerConfig.containsKey(producerName)) {
            log.warn("User configured a 'producerName' in the config, but this can cause errors if multiple pods spin "
                    + "up with the same name. Overriding with '{}'", podName);
        }
        producerConfig.put(producerName, podName);

        return pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .loadConf(producerConfig)
                .create();
    }
}