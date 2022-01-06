package io.openmessaging.benchmark.driver.chronicle;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ChronicleBenchmarkProducer implements BenchmarkProducer {
    private final ChronicleQueue queue;
    private final ScheduledExecutorService executor;

    public ChronicleBenchmarkProducer(SingleChronicleQueue queue, ScheduledExecutorService executor) {
        this.queue = queue;
        this.executor = executor;
        this.executor.schedule(this::pretouch, 100, TimeUnit.MILLISECONDS);
    }

    private void pretouch() {
        queue.acquireAppender().pretouch();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
            final Bytes<?> bytes = dc.wire().bytes();
            bytes.writeInt(0);
            bytes.writeLong(System.currentTimeMillis());
            bytes.writeStopBit(payload.length);
            bytes.write(payload);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        // nothing to close
    }
}
