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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

/**
 * SimpleSource is a simple implementation of Sourcer.
 * It generates messages with increasing offsets.
 * Keeps track of the offsets of the messages read and
 * acknowledges them when ack is called.
 */

@Slf4j
@Component
public class SimpleSource extends Sourcer {
    private final Map<Long, Boolean> messages = new ConcurrentHashMap<>();
    private long readIndex = 0;

    private Server server;

    @PostConstruct // starts server automatically when the spring context initializes
    public void startServer() throws Exception {
        server = new Server(this);
        server.start();
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        long startTime = System.currentTimeMillis();
        if (messages.entrySet().size() > 0) {
            // if there are messages not acknowledged, return
            return;
        }

        for (int i = 0; i < request.getCount(); i++) {
            if (System.currentTimeMillis() - startTime > request.getTimeout().toMillis()) {
                return;
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("x-txn-id", UUID.randomUUID().toString());

            // create a message with increasing offset
            Offset offset = new Offset(Longs.toByteArray(readIndex));
            Message message = new Message(
                    Long.toString(readIndex).getBytes(),
                    offset,
                    Instant.now(),
                    headers);
            // send the message to the observer
            observer.send(message);
            // keep track of the messages read and not acknowledged
            messages.put(readIndex, true);
            readIndex += 1;
        }
    }

    @Override
    public void ack(AckRequest request) {
        for (Offset offset : request.getOffsets()) {
            Long decoded_offset = Longs.fromByteArray(offset.getValue());
            // remove the acknowledged messages from the map
            messages.remove(decoded_offset);
        }
    }

    @Override
    public long getPending() {
        // number of messages not acknowledged yet
        return messages.size();
    }

    @Override
    public List<Integer> getPartitions() {
        return Sourcer.defaultPartitions();
    }
}