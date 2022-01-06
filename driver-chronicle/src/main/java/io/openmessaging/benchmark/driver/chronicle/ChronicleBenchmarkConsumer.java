package io.openmessaging.benchmark.driver.chronicle;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;

public class ChronicleBenchmarkConsumer implements BenchmarkConsumer {

    private final ChronicleQueue queue;
    private final int consumerId;
    private final ConsumerCallback consumerCallback;
    private EventLoop eventLoop;
    private volatile boolean running = true;
    private byte[] byteArray = {};
    private ExcerptTailer tailer;

    public ChronicleBenchmarkConsumer(String topic, ConsumerCallback consumerCallback, SingleChronicleQueue queue, EventLoop eventLoop, int consumerId) {
        this.consumerCallback = consumerCallback;
        this.queue = queue;
        this.consumerId = consumerId;
        if (eventLoop == null) {
            this.eventLoop = eventLoop = new MediumEventLoop(null, "consumer-" + topic, Pauser.balanced(), true, "none");
            eventLoop.start();
        }
        eventLoop.addHandler(this::run);
    }

    boolean run() throws InvalidEventHandlerException {
        if (tailer == null)
            tailer = queue.createTailer();
        if (!running) {
            Closeable.closeQuietly(tailer);
            throw new InvalidEventHandlerException("Stopped");
        }

        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            final Bytes<?> bytes = dc.wire().bytes();
            if (!bytes.compareAndSwapInt(bytes.readPosition(), 0, consumerId))
                return true;
            int id = bytes.readInt();
            long time = bytes.readLong();
            int length = bytes.readStopBitChar();
            if (length != byteArray.length)
                byteArray = new byte[length];
            bytes.read(byteArray);
            consumerCallback.messageReceived(byteArray, time);
            return true;
        }
    }

    @Override
    public void close() {
        running = false;
        Closeable.closeQuietly(eventLoop);
    }
}
