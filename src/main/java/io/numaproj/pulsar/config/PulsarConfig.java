package io.numaproj.pulsar.config;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;
import java.util.UUID;

@Configuration
public class PulsarConfig {

    @Autowired
    private Environment env;

    /**
     * Creates a PulsarClient bean using the provided configuration properties.
     *
     * @param properties The Pulsar client configuration properties
     * @return A configured PulsarClient instance
     * @throws PulsarClientException if client creation fails
     */
    @Bean
    public PulsarClient pulsarClient(PulsarClientProperties properties) throws PulsarClientException {
        Map<String, Object> config = properties.getClientConfig();
        
        // Validate required configuration
        if (config == null || !config.containsKey("serviceUrl")) {
            throw new IllegalArgumentException("serviceUrl is required for Pulsar client configuration");
        }
        
        String serviceUrl = (String) config.get("serviceUrl");
        return PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .loadConf(config)
                .build();
    }

    /**
     * Creates a Pulsar Producer bean using the provided client and configuration properties.
     *
     * @param client The PulsarClient instance
     * @param properties The Pulsar producer configuration properties
     * @return A configured Producer instance
     * @throws PulsarClientException if producer creation fails
     */
    @Bean
    public Producer<byte[]> pulsarProducer(PulsarClient client, PulsarProducerProperties properties) 
            throws PulsarClientException {
        Map<String, Object> config = properties.getProducerConfig();
        
        // Validate required configuration
        if (config == null || !config.containsKey("topicName")) {
            throw new IllegalArgumentException("topicName is required for Pulsar producer configuration");
        }
        
        // Set producer name if not explicitly configured
        if (!config.containsKey("producerName")) {
            // Try to get from NUMAFLOW_POD environment variable
            String podName = env.getProperty("NUMAFLOW_POD", (String) null);
            
            if (podName != null) {
                config.put("producerName", podName);
            } else {
                // Generate a fallback name with "pod-" prefix and a random UUID
                config.put("producerName", "pod-" + UUID.randomUUID().toString());
            }
        }
        
        return client.newProducer(Schema.BYTES)
                .loadConf(config)
                .create();
    }
    
    /**
     * Creates a Pulsar Consumer bean using the provided client and configuration properties.
     *
     * @param client The PulsarClient instance
     * @param properties The Pulsar consumer configuration properties
     * @return A configured Consumer instance
     * @throws PulsarClientException if consumer creation fails
     */
    @Bean
    public Consumer<byte[]> pulsarConsumer(PulsarClient client, PulsarConsumerProperties properties) 
            throws PulsarClientException {
        Map<String, Object> config = properties.getConsumerConfig();
        
        // Validate required configuration
        if (config == null || !config.containsKey("topicNames") && !config.containsKey("topicsPattern")) {
            throw new IllegalArgumentException("Either topicNames or topicsPattern is required for Pulsar consumer configuration");
        }
        
        if (config == null || !config.containsKey("subscriptionName")) {
            throw new IllegalArgumentException("subscriptionName is required for Pulsar consumer configuration");
        }
        
        // Set consumer name if not explicitly configured
        if (!config.containsKey("consumerName")) {
            // Try to get from NUMAFLOW_POD environment variable
            String podName = env.getProperty("NUMAFLOW_POD", (String) null);
            
            if (podName != null) {
                config.put("consumerName", podName);
            } else {
                // Generate a fallback name with "consumer-" prefix and a random UUID
                config.put("consumerName", "consumer-" + UUID.randomUUID().toString());
            }
        }
        
        return client.newConsumer(Schema.BYTES)
                .loadConf(config)
                .subscribe();
    }
}