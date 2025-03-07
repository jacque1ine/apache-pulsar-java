package io.numaproj.pulsar.consumer;

import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.Sourcer;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.BatchReceivePolicy;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "spring.pulsar.consumer", name = "enabled", havingValue = "true")
public class PulsarSource extends Sourcer {

    // Mapping of readIndex to Pulsar messages that haven't yet been acknowledged.
    private final Map<String, org.apache.pulsar.client.api.Message<byte[]>> messages = new ConcurrentHashMap<>();
    private Server server;

    @Autowired
    private PulsarClient pulsarClient;

    // Consumer for Pulsar messages on the hardcoded topic "demo-t".
    private Consumer<byte[]> pulsarConsumer;

    @PostConstruct // starts server automatically when the Spring context initializes.
    public void startServer() throws Exception {
        // Create and start Pulsar consumer using the autowired PulsarClient.

        BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .timeout(5000, TimeUnit.MILLISECONDS)
                .build();

        pulsarConsumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic("tester")
                .subscriptionName("sub")
                .batchReceivePolicy(batchReceivePolicy)
                .subscribe();

        // Start the gRPC server for reading messages.
        server = new Server(this);
        server.start();
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        if (!messages.isEmpty()) {
            return;
        }
        int messagesRead = 0;
        long overallTimeoutMillis = request.getTimeout().toMillis();
        long deadline = System.currentTimeMillis() + overallTimeoutMillis;
        // Loop until we have processed the requested count or run out of time.
        while (messagesRead < request.getCount()) {
            long iterationStart = System.currentTimeMillis();
            long remainingTime = deadline - iterationStart;
            if (remainingTime <= 0) {
                log.info("Overall timeout reached before requested count was met.");
                break;
            }
            List<org.apache.pulsar.client.api.Message<byte[]>> batchMsgs = new ArrayList<>();
            try {
                Messages<byte[]> msgs = pulsarConsumer.batchReceive();
                if (msgs != null && msgs.size() > 0) {
                    for (org.apache.pulsar.client.api.Message<byte[]> msg : msgs) {
                        if (msg != null) {
                            batchMsgs.add(msg);
                        }
                    }
                }
            } catch (PulsarClientException e) {
                log.error("Error while consuming messages from Pulsar", e);
                return;
            }
            // If no messages were received in this iteration, update remaining time and
            // continue.
            if (batchMsgs.isEmpty()) {
                long elapsed = System.currentTimeMillis() - iterationStart;
                deadline = deadline - elapsed;
                continue;
            }
            // Process the messages in the batch.
            for (org.apache.pulsar.client.api.Message<byte[]> pMsg : batchMsgs) {
                if (messagesRead >= request.getCount()) {
                    break;
                }
                // Log the consumed message (converting byte[] to String for clarity).
                log.info("Consumed Pulsar message: {}", new String(pMsg.getValue(), StandardCharsets.UTF_8));

                // Convert the Pulsar MessageId to a byte array using its String representation.
                byte[] offsetBytes = pMsg.getMessageId().toString().getBytes(StandardCharsets.UTF_8);
                Offset offset = new Offset(offsetBytes);

                // Create a header map containing the Pulsar message id detail.
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
            long iterationElapsed = System.currentTimeMillis() - iterationStart;
            // Update deadline by reducing the elapsed time.
            deadline -= iterationElapsed;
        }
        log.debug("Read request count:{} number of messages sent:{}", request.getCount(), messagesRead);
    }

    @Override
    public void ack(AckRequest request) {
        // Iterate over provided offsets and acknowledge the corresponding Pulsar
        // message.
        request.getOffsets().forEach(offset -> {
            // Convert the offset bytes back to a String to obtain the Pulsar MessageId.
            String messageIdKey = new String(offset.getValue(), StandardCharsets.UTF_8);
            org.apache.pulsar.client.api.Message<byte[]> pMsg = messages.get(messageIdKey);
            if (pMsg != null) {
                try {
                    pulsarConsumer.acknowledge(pMsg);
                    // Log both the MessageId and payload, converting the byte[] to String for
                    // clarity.
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
        // Return the number of messages that have not yet been acknowledged.
        return messages.size();
    }

    @Override
    public List<Integer> getPartitions() {
        // Fallback mechanism: a single partition based on the pod replica index.
        return Sourcer.defaultPartitions();
    }
}