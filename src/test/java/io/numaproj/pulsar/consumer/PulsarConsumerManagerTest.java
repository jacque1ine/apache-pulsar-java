package io.numaproj.pulsar.consumer;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import io.numaproj.pulsar.config.consumer.PulsarConsumerProperties;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

public class PulsarConsumerManagerTest {

    private PulsarConsumerManager manager;
    private PulsarConsumerProperties consumerProperties;
    private PulsarClient mockPulsarClient;
    private ConsumerBuilder<byte[]> mockConsumerBuilder;
    private Consumer<byte[]> mockConsumer;

    @Before
    public void setUp() {
        // Create a simple consumer properties object with a dummy config
        consumerProperties = new PulsarConsumerProperties();
        Map<String, Object> config = new HashMap<>();
        config.put("dummyKey", "dummyValue");
        consumerProperties.setConsumerConfig(config);

        // Instantiate the manager and inject dependencies using ReflectionTestUtils
        manager = new PulsarConsumerManager();
        ReflectionTestUtils.setField(manager, "pulsarConsumerProperties", consumerProperties);

        // Create mocks for PulsarClient and the ConsumerBuilder chain
        mockPulsarClient = mock(PulsarClient.class);
        mockConsumerBuilder = mock(ConsumerBuilder.class);
        mockConsumer = mock(Consumer.class);
        ReflectionTestUtils.setField(manager, "pulsarClient", mockPulsarClient);
    }

    @After
    public void tearDown() {
        manager = null;
        consumerProperties = null;
        mockPulsarClient = null;
        mockConsumerBuilder = null;
        mockConsumer = null;
    }

    @Test
    public void getOrCreateConsumer_createsNewConsumer() {
        try {
            // Set up the chaining calls on the ConsumerBuilder mock
            when(mockPulsarClient.newConsumer(Schema.BYTES)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.loadConf(anyMap())).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.batchReceivePolicy(any(BatchReceivePolicy.class))).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscriptionType(SubscriptionType.Shared)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

            // Call getOrCreateConsumer for the first time so it creates a new consumer
            Consumer<byte[]> firstConsumer = manager.getOrCreateConsumer(10L, 1000L);
            assertNotNull("A consumer should be created", firstConsumer);
            assertEquals("The returned consumer should be the mock consumer", mockConsumer, firstConsumer);

            // Call again and verify that it returns the same instance (i.e.,
            // builder.subscribe() is not called again)
            Consumer<byte[]> secondConsumer = manager.getOrCreateConsumer(10L, 1000L);
            assertEquals("Should return the same consumer instance", firstConsumer, secondConsumer);

            // Verify that newConsumer(...) and subscribe() are invoked only once
            verify(mockPulsarClient, times(1)).newConsumer(Schema.BYTES);
            verify(mockConsumerBuilder, times(1)).subscribe();

            // Capture loaded configuration to verify that consumerProperties configuration
            // is passed
            ArgumentCaptor<Map> configCaptor = ArgumentCaptor.forClass(Map.class);
            verify(mockConsumerBuilder).loadConf(configCaptor.capture());
            Map loadedConfig = configCaptor.getValue();
            assertEquals("dummyValue", loadedConfig.get("dummyKey"));

            ArgumentCaptor<BatchReceivePolicy> batchPolicyCaptor = ArgumentCaptor.forClass(BatchReceivePolicy.class);
            verify(mockConsumerBuilder).batchReceivePolicy(batchPolicyCaptor.capture());
            BatchReceivePolicy builtPolicy = batchPolicyCaptor.getValue();
            assertNotNull("BatchReceivePolicy should be set", builtPolicy);

            // Validate maxNumMessages and timeoutMillis configurations
            assertEquals("BatchReceivePolicy should have maxNumMessages set to 10", 10,
                    builtPolicy.getMaxNumMessages());
            assertEquals("BatchReceivePolicy should have timeout set to 1000ms", 1000, builtPolicy.getTimeoutMs());
        } catch (PulsarClientException e) {
            fail("Unexpected PulsarClientException thrown: " + e.getMessage());
        }
    }

    @Test
    public void cleanup_closesConsumerAndClient() {
        try {
            // Set up the Consumer to be non-null so that cleanup closes it
            when(mockPulsarClient.newConsumer(Schema.BYTES)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.loadConf(anyMap())).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.batchReceivePolicy(any(BatchReceivePolicy.class))).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscriptionType(SubscriptionType.Shared)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

            // Create the consumer via getOrCreateConsumer
            Consumer<byte[]> createdConsumer = manager.getOrCreateConsumer(5L, 500L);
            assertNotNull(createdConsumer);

            // Call cleanup and verify that close() is called on both consumer and client
            manager.cleanup();
            verify(createdConsumer, times(1)).close();
            verify(mockPulsarClient, times(1)).close();
        } catch (PulsarClientException e) {
            fail("Unexpected PulsarClientException thrown during test cleanup_closesConsumerAndClient: "
                    + e.getMessage());
        }
    }

    @Test
    public void cleanup_whenConsumerIsNull() {
        try {
            // Set currentConsumer to null explicitly
            ReflectionTestUtils.setField(manager, "currentConsumer", null);

            // Call cleanup, expecting that the client is closed even if consumer is null
            manager.cleanup();
            verify(mockPulsarClient, times(1)).close();
        } catch (PulsarClientException e) {
            fail("Unexpected PulsarClientException thrown during test cleanup_whenConsumerIsNull: " + e.getMessage());
        }
    }

    @Test
    public void cleanup_consumerCloseThrowsException() {
        try {
            // Setup: create a consumer and simulate an exception on closing consumer
            when(mockPulsarClient.newConsumer(Schema.BYTES)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.loadConf(anyMap())).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.batchReceivePolicy(any(BatchReceivePolicy.class))).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscriptionType(SubscriptionType.Shared)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

            Consumer<byte[]> createdConsumer = manager.getOrCreateConsumer(3L, 300L);
            assertNotNull(createdConsumer);

            // Simulate exception when consumer.close() is invoked
            doThrow(new PulsarClientException("Consumer close failed")).when(createdConsumer).close();

            // Call cleanup; should catch the exception and still proceed to close the
            // client
            manager.cleanup();
            verify(createdConsumer, times(1)).close();
            verify(mockPulsarClient, times(1)).close();
        } catch (PulsarClientException e) {
            fail("Unexpected PulsarClientException thrown during test cleanup_consumerCloseThrowsException: "
                    + e.getMessage());
        }
    }

    @Test
    public void cleanup_clientCloseThrowsException() {
        try {
            // Set up consumer as null so that only client.close() is invoked during cleanup
            ReflectionTestUtils.setField(manager, "currentConsumer", null);

            // Simulate exception when pulsarClient.close() is invoked
            doThrow(new PulsarClientException("Client close failed")).when(mockPulsarClient).close();

            // Call cleanup; should catch the exception
            manager.cleanup();
            verify(mockPulsarClient, times(1)).close();
        } catch (PulsarClientException e) {
            fail("Unexpected PulsarClientException thrown during test cleanup_clientCloseThrowsException: "
                    + e.getMessage());
        }
    }

}
