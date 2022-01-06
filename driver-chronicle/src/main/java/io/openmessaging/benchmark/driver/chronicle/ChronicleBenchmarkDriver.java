/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.chronicle;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.threads.MediumEventLoop;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.posix.PosixAPI;
import org.apache.bookkeeper.stats.StatsLogger;
import software.chronicle.enterprise.queue.ChronicleRingBuffer;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static net.openhft.chronicle.wire.WireType.TEXT;

@UsedViaReflection
public class ChronicleBenchmarkDriver implements BenchmarkDriver {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final List<BenchmarkProducer> producers = Collections.synchronizedList(new ArrayList<>());
    private final List<BenchmarkConsumer> consumers = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, SingleChronicleQueue> queueMap = new LinkedHashMap<>();
    private EventLoop eventLoop;
    private Config config;
    private ScheduledExecutorService executor;
    private BufferMode bufferedMode;
    private int consumersCount = 0;
    private String bufferPath;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, Config.class);
        IOTools.deleteDirWithFiles(config.path);

        bufferPath = config.bufferPath;
        eventLoop = new MediumEventLoop(null, "flushers", Pauser.busy(), true, "any");
        eventLoop.start();

        executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("producer-pretoucher", true));

        bufferedMode = ObjectUtils.convertTo(BufferMode.class, config.bufferMode);

        System.out.println(TEXT.asString(config));
        IOTools.deleteDirWithFiles(bufferPath);
        new File(bufferPath).mkdirs();
    }

    @Override
    public String getTopicNamePrefix() {
        return "bench";
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        final int bufferCapacity = 128 << 20;
        ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreator = (capacity, maxTailers) -> {
            long length = ChronicleRingBuffer.sizeFor(bufferCapacity, 2); // MB
            MappedBytes mappedBytes = MappedBytes.singleMappedBytes(new File(bufferPath, topic), length);
            final long addr = mappedBytes.addressForRead(0);
            PosixAPI.posix().mlock(addr, length, true);
            return mappedBytes;
        };

        final File queueDir = new File(config.path, topic);
        createQueue(queueDir, topic, bufferCapacity, bufferBytesStoreCreator, bufferedMode);
        if (bufferedMode == BufferMode.Asynchronous)
            createQueue(queueDir, topic, bufferCapacity, bufferBytesStoreCreator, BufferMode.None);
        return CompletableFuture.completedFuture(null);
    }

    private void createQueue(File queueDir, String topic, int bufferCapacity, ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreator, BufferMode bufferedMode) {
        SingleChronicleQueue queue = ChronicleQueue.singleBuilder(queueDir)
                .useSparseFiles(true)
                .readBufferMode(bufferedMode)
                .writeBufferMode(bufferedMode)
                .bufferCapacity(bufferCapacity)
                .bufferBytesStoreCreator(bufferBytesStoreCreator)
                .eventLoop(eventLoop)
                .build();
        String key = topic;
        if (bufferedMode == BufferMode.Asynchronous)
            key += "-async";
        queueMap.put(key, queue);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        final String topic2 = bufferedMode == BufferMode.Asynchronous ? topic + "-async" : topic;
        BenchmarkProducer benchmarkProducer = new ChronicleBenchmarkProducer(queueMap.get(topic2), executor);
        // Add to producer list to close later
        producers.add(benchmarkProducer);
        return CompletableFuture.completedFuture(benchmarkProducer);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
                                                               ConsumerCallback consumerCallback) {
        SingleChronicleQueue queue = queueMap.get(topic);
        final int consumerId = ++consumersCount;
        BenchmarkConsumer benchmarkConsumer = new ChronicleBenchmarkConsumer(topic, consumerCallback, queue, null, consumerId);
        // Add to producer list to close later
        consumers.add(benchmarkConsumer);
        return CompletableFuture.completedFuture(benchmarkConsumer);
    }

    @Override
    public void close() {
        Closeable.closeQuietly(producers);
        Closeable.closeQuietly(consumers);
        Closeable.closeQuietly(queueMap.values());
        Closeable.closeQuietly(eventLoop);
    }
}
