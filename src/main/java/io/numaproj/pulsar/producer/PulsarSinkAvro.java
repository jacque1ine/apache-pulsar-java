
package io.numaproj.pulsar.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

@Slf4j
// @Component
// @ConditionalOnProperty(prefix = "spring.pulsar.producer", name = "enabled", havingValue = "true")
public class PulsarSinkAvro extends Sinker {

    @Autowired
    private Producer<byte[]> producer;

    @Autowired
    private PulsarClient pulsarClient;

    private Server server;

    @PostConstruct // starts server automatically when the spring context initializes
    public void startServer() throws Exception {
        server = new Server(this);
        server.start();
        server.awaitTermination();
    }

    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Load the Avro schema
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("path/to/your/schema.avsc"));
        } catch (IOException e) {
            // Log and possibly return an error response if the schema can't be loaded
            log.error("Failed to parse Avro schema file. Messages cannot be processed.", e);
            return responseListBuilder.build();
        }

        while (true) {
            Datum datum;
            try {
                datum = datumIterator.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            if (datum == null) {
                break;
            }

            final byte[] msg = datum.getValue();
            final String msgId = datum.getId();

            try {
                // Decode the incoming message from Base64.
                // Assumes that the message bytes represent a Base64-encoded UTF-8 string.
                String encodedMessage = new String(msg, StandardCharsets.UTF_8);
                byte[] decodedMsg = Base64.getDecoder().decode(encodedMessage);

                // Deserialize decoded byte[] into GenericRecord
                Decoder decoder = DecoderFactory.get().binaryDecoder(decodedMsg, null);
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                GenericRecord avroRecord = reader.read(null, decoder);

                // Process your avroRecord as needed here...

                // Serialize if needed back to byte[]
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                writer.write(avroRecord, encoder);
                encoder.flush();

                byte[] avroMsg = outputStream.toByteArray();

                // Send asynchronously
                CompletableFuture<Void> future = producer.sendAsync(avroMsg)
                        .thenAccept(messageId -> {
                            log.info("Processed message ID: {}, Content: {}", msgId, avroRecord);
                            responseListBuilder.addResponse(Response.responseOK(msgId));
                        })
                        .exceptionally(ex -> {
                            log.error("Error processing message ID {}: {}", msgId, ex.getMessage(), ex);
                            responseListBuilder.addResponse(Response.responseFailure(msgId, ex.getMessage()));
                            return null;
                        });

                futures.add(future);
            } catch (IOException e) {
                // If Avro read/write errors occur, log and mark as failure
                log.error("IOException occurred while processing Avro message with ID {}: {}",
                        msgId, e.getMessage(), e);
                responseListBuilder.addResponse(Response.responseFailure(msgId, e.getMessage()));
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return responseListBuilder.build();
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (producer != null) {
                producer.close();
                log.info("Producer closed.");
            }
            if (pulsarClient != null) {
                pulsarClient.close();
                log.info("PulsarClient closed.");
            }
        } catch (PulsarClientException e) {
            log.error("Error while closing PulsarClient or Producer.", e);
        }
    }
}
