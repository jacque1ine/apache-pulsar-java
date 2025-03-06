package io.numaproj.pulsar.consumer;

import com.google.common.primitives.Longs;
import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.Sourcer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "spring.pulsar.consumer", name = "enabled", havingValue = "true")
public class PulsarSource extends Sourcer {

    // Mapping of readIndex to Pulsar messages that haven't yet been acknowledged
    private final Map<String, org.apache.pulsar.client.api.Message<byte[]>> messages = new ConcurrentHashMap<>();
    private Server server;

    @Autowired
    private PulsarClient pulsarClient;

    // Consumer for Pulsar messages on hardcoded topic "demo-t"
    private Consumer<byte[]> pulsarConsumer;

    @PostConstruct // starts server automatically when the Spring context initializes
    public void startServer() throws Exception {
        // Create and start Pulsar consumer using the autowired PulsarClient.
        pulsarConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic("demo-t")
                .subscriptionName("my-topic-subscription")
                .subscribe();

        // Start the gRPC server for reading messages.
        server = new Server(this);
        server.start();
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        // Minimal check: if outstanding messages haven't been acknowledged, do nothing.
        if (!messages.isEmpty()) {
            return;
        }
        try {
            // Use batchReceive without timeout parameters as the API does not support them.
            Messages<byte[]> batch = pulsarConsumer.batchReceive();
            if (batch == null || batch.size() == 0) {
                // No messages received.
                return;
            }

            int messagesRead = 0;
            // Process messages up to the specified count.
            for (org.apache.pulsar.client.api.Message<byte[]> pMsg : batch) {
                if (messagesRead >= request.getCount()) {
                    break;
                }
                // Log the consumed message (converting byte[] to String for clarity)
                log.info("Consumed Pulsar message: {}", new String(pMsg.getValue(), StandardCharsets.UTF_8));

                // Convert the Pulsar MessageId to a byte array using its String representation.
                byte[] offsetBytes = pMsg.getMessageId().toString().getBytes(StandardCharsets.UTF_8);
                Offset offset = new Offset(offsetBytes);

                // Create a header map with Pulsar message id detail.
                Map<String, String> headers = new HashMap<>();
                headers.put("pulsarMessageId", pMsg.getMessageId().toString());

                // Create a new message wrapping the received Pulsar message value.
                Message message = new Message(pMsg.getValue(), offset, Instant.now(), headers);

                // Send the message to the observer.
                observer.send(message);

                // Track the Pulsar message using its MessageId as the key.
                messages.put(pMsg.getMessageId().toString(), pMsg);

                messagesRead++;
            }
        } catch (PulsarClientException e) {
            log.error("Failed to receive batch from Pulsar", e);
        }
    }

    @Override
    public void ack(AckRequest request) {
        // Iterate over provided offsets and acknowledge the corresponding Pulsar message.
        request.getOffsets().forEach(offset -> {
            // Convert the offset bytes back to String to get the Pulsar MessageId string.
            String messageIdKey = new String(offset.getValue(), StandardCharsets.UTF_8);
            org.apache.pulsar.client.api.Message<byte[]> pMsg = messages.get(messageIdKey);
            if (pMsg != null) {
                try {
                    pulsarConsumer.acknowledge(pMsg);
                    // Log both the MessageId and payload using UTF-8 conversion for the byte array.
                    log.info("Acknowledged Pulsar message with ID: {} and payload: {}",
                            pMsg.getMessageId().toString(), new String(pMsg.getValue(), StandardCharsets.UTF_8));
                } catch (PulsarClientException e) {
                    log.error("Failed to acknowledge Pulsar message", e);
                }
                // Remove the acknowledged message from the tracking map.
                messages.remove(messageIdKey);
            }
        });
    }

    @Override
    public long getPending() {
        // Number of messages not yet acknowledged.
        return messages.size();
    }

    @Override
    public List<Integer> getPartitions() {
        // Fallback mechanism: a single partition based on your pod replica index.
        return Sourcer.defaultPartitions();
    }
}