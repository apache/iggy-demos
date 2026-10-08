/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.kafka;

import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.kafka.RecordCodec.KafkaRecord;
import org.apache.iggy.kafka.RecordCodec.Window;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendConfirmation;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Producer} that writes to Apache Iggy.
 *
 * <p>Use it in place of {@code KafkaProducer}: the constructors take the same arguments, and
 * {@code bootstrap.servers}, {@code key.serializer}, {@code value.serializer}, {@code linger.ms}
 * {@code buffer.memory}, {@code max.block.ms} and {@code client.id} keep their Kafka meaning. Records
 * are batched per partition on a background thread, and callbacks run on that thread, as they do in
 * Kafka. When {@code buffer.memory} bytes are waiting to be sent, {@code send} blocks for up to
 * {@code max.block.ms} and then fails the record with a {@link TimeoutException}.
 *
 * <p>Keyed records go to {@code murmur2(key) % partitions}, the same partition Kafka's default
 * partitioner picks, using a partition count refreshed every {@code metadata.max.age.ms}. Unkeyed
 * records are sent with Iggy's balanced partitioning: the Iggy SDK spreads each batch over the
 * partitions in turn, with a partition count it refreshes every few seconds. Transactions are not
 * supported.
 */
public class IggyKafkaProducer<K, V> implements Producer<K, V> {

    /** Most bytes the sender takes per round, across partitions; each partition still gets one request. */
    private static final long MAX_ROUND_BYTES = 4L * 1024 * 1024;
    /** Most records the sender drains under one queue lock; the byte cap is checked after each chunk. */
    private static final int DRAIN_CHUNK = 64;

    private static final String GROUP = "producer-metrics";
    /** Per-record overhead counted against the buffer: the Iggy message header plus the mapping version header. */
    private static final int RECORD_OVERHEAD = MessageHeader.SIZE + 16;

    private final IggyConnection connection;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final long lingerMs;
    private final long bufferMemory;
    private final long maxBlockMs;
    private final boolean autoCreateTopics;
    private final BlockingQueue<Pending> queue = new LinkedBlockingQueue<>();
    private final Object inFlightLock = new Object();
    private final Thread sender;
    private final ClientMetrics metrics;
    private final Sensor sendSensor;
    private final Sensor errorSensor;
    private final Sensor latencySensor;
    private long inFlight;
    private long bufferedBytes;
    private volatile boolean closed;
    /** Set when close() runs on the sender thread itself, which then closes the resources as it ends. */
    private volatile boolean closeOnSenderExit;

    public IggyKafkaProducer(Properties properties) {
        this(IggyKafkaConfig.toMap(properties), null, null);
    }

    public IggyKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(IggyKafkaConfig.toMap(properties), keySerializer, valueSerializer);
    }

    public IggyKafkaProducer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public IggyKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        IggyKafkaConfig config = new IggyKafkaConfig(configs);
        this.keySerializer = keySerializer != null ? keySerializer : Plugins.create(config, "key.serializer", true);
        this.valueSerializer =
                valueSerializer != null ? valueSerializer : Plugins.create(config, "value.serializer", false);
        this.lingerMs = config.longValue("linger.ms", 5);
        this.bufferMemory = config.longValue("buffer.memory", 32 * 1024 * 1024L);
        this.maxBlockMs = config.longValue("max.block.ms", 60_000);
        this.autoCreateTopics = config.autoCreateTopics();
        this.connection = new IggyConnection(config);
        String clientId = config.string("client.id", "iggy-producer");
        this.metrics = new ClientMetrics(clientId);
        this.sendSensor = metrics.totalAndRate(GROUP, "record-send", "records sent");
        this.errorSensor = metrics.totalAndRate(GROUP, "record-error", "records that failed to send");
        this.latencySensor = metrics.average(GROUP, "request-latency", "send request latency in ms");
        metrics.gauge(GROUP, "buffer-available-bytes", "Buffer memory not in use", this::bufferAvailable);
        this.sender = new Thread(this::runSender, "iggy-kafka-producer-network-thread | " + clientId);
        this.sender.setDaemon(true);
        this.sender.start();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * As {@code KafkaProducer.send}: a closed producer throws, everything else fails the record's
     * future, including a partition the topic does not have. A timestamp Iggy cannot store fails the
     * record here, so the sender thread never meets one.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        if (closed) {
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
        }
        byte[] key;
        byte[] value;
        try {
            key = keySerializer.serialize(record.topic(), record.headers(), record.key());
            value = valueSerializer.serialize(record.topic(), record.headers(), record.value());
        } catch (RuntimeException e) {
            throw new SerializationException("Can't serialize record for topic " + record.topic(), e);
        }
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        try {
            int partition = partition(record, key);
            long timestamp = record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
            RecordCodec.timestampIn(timestamp); // Refused here, so the sender thread never meets one.
            KafkaRecord kafkaRecord = KafkaRecord.of(key, value, record.headers(), timestamp);
            int size = size(kafkaRecord);
            enqueue(new Pending(new TopicPartition(record.topic(), partition), kafkaRecord, size, future, callback));
        } catch (InterruptException | ClosedWhileSending | IllegalArgumentException | IllegalStateException e) {
            throw e; // Kafka throws these from send() rather than failing the record.
        } catch (RuntimeException e) {
            return fail(future, callback, record, e);
        }
        return future;
    }

    private long bufferAvailable() {
        synchronized (inFlightLock) {
            return bufferMemory - bufferedBytes;
        }
    }

    private static int size(KafkaRecord record) {
        long size = RECORD_OVERHEAD;
        size += record.key() == null ? 0 : record.key().length;
        size += record.value() == null ? 0 : record.value().length;
        for (Header header : record.headers()) {
            size += header.key().length() + (header.value() == null ? 0 : header.value().length);
        }
        return (int) Math.min(size, Integer.MAX_VALUE);
    }

    /**
     * Waits until the record fits in {@code buffer.memory}, as Kafka's accumulator does, then
     * queues it. The reservation and the queue add happen under the lock that close() takes, so a
     * record is either queued before the sender sees {@code closed} or refused.
     */
    private void enqueue(Pending pending) {
        int size = pending.size;
        if (size > bufferMemory) {
            throw new RecordTooLargeException("The message is " + size
                    + " bytes when serialized which is larger than the total memory buffer you have configured"
                    + " with the buffer.memory configuration.");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxBlockMs);
        synchronized (inFlightLock) {
            while (bufferedBytes + size > bufferMemory && !closed) {
                long waitMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (waitMs <= 0) {
                    throw new TimeoutException(
                            "Failed to allocate memory within the configured max blocking time " + maxBlockMs + " ms.");
                }
                try {
                    inFlightLock.wait(waitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptException(e);
                }
            }
            if (closed) {
                throw new ClosedWhileSending();
            }
            bufferedBytes += size;
            inFlight++;
            queue.add(pending);
        }
    }

    private int partition(ProducerRecord<K, V> record, byte[] key) {
        int partitions = Math.toIntExact(connection.partitionCount(record.topic()));
        if (record.partition() != null) {
            if (record.partition() >= partitions) {
                connection.invalidate(record.topic());
                partitions = Math.toIntExact(connection.partitionCount(record.topic()));
            }
            if (record.partition() >= partitions) {
                throw new TimeoutException("Partition " + record.partition() + " of topic " + record.topic()
                        + " with partition count " + partitions + " is not present in metadata.");
            }
            return record.partition();
        }
        if (key != null) {
            return Utils.toPositive(Utils.murmur2(key)) % partitions;
        }
        return RecordMetadata.UNKNOWN_PARTITION; // The SDK spreads the batch over the partitions.
    }

    private Future<RecordMetadata> fail(
            CompletableFuture<RecordMetadata> future, Callback callback, ProducerRecord<K, V> record, Exception e) {
        Exception error = wrap(e);
        future.completeExceptionally(error);
        if (callback != null) {
            int p = record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition();
            callback.onCompletion(new RecordMetadata(new TopicPartition(record.topic(), p), -1, -1, -1, -1, -1), error);
        }
        return future;
    }

    private void runSender() {
        try {
            List<Pending> batch = new ArrayList<>();
            while (!closed || !queue.isEmpty()) {
                try {
                    Pending first = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (first == null) {
                        continue;
                    }
                    batch.add(first);
                    long bytes = first.size;
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lingerMs);
                    while (bytes < MAX_ROUND_BYTES) {
                        int from = batch.size();
                        if (queue.drainTo(batch, DRAIN_CHUNK) == 0) {
                            long wait = deadline - System.nanoTime();
                            Pending next = wait > 0 ? queue.poll(wait, TimeUnit.NANOSECONDS) : null;
                            if (next == null) {
                                break;
                            }
                            batch.add(next);
                        }
                        for (int i = from; i < batch.size(); i++) {
                            bytes += batch.get(i).size;
                        }
                    }
                } catch (InterruptedException e) {
                    if (closed) {
                        // close(timeout) ran out of time: fail what is still queued instead of sending it.
                        queue.drainTo(batch);
                        for (Pending p : batch) {
                            complete(
                                    p,
                                    unknownOffset(p.partition, p),
                                    new KafkaException("Producer is closed forcefully."));
                        }
                        batch.clear();
                        continue;
                    }
                }
                sendBatch(batch);
                batch.clear();
            }
        } finally {
            if (closeOnSenderExit) {
                closeResources();
            }
        }
    }

    private void sendBatch(List<Pending> batch) {
        Map<TopicPartition, List<Pending>> byPartition = new LinkedHashMap<>();
        for (Pending pending : batch) {
            byPartition
                    .computeIfAbsent(pending.partition, tp -> new ArrayList<>())
                    .add(pending);
        }
        for (Map.Entry<TopicPartition, List<Pending>> entry : byPartition.entrySet()) {
            TopicPartition requested = entry.getKey();
            TopicPartition tp = requested;
            List<Pending> group = entry.getValue();
            List<Message> messages = new ArrayList<>(group.size());
            List<KafkaRecord> batchRecords = new ArrayList<>(group.size());
            for (Pending p : group) {
                batchRecords.add(p.record);
            }
            // One window per send: Iggy holds origin timestamps at most u32::MAX microseconds apart.
            // send() refused every timestamp Iggy cannot hold, so this cannot throw.
            Window window = Window.of(batchRecords);
            Exception[] unconverted = new Exception[group.size()];
            for (int i = 0; i < group.size(); i++) {
                try {
                    messages.add(RecordCodec.toMessage(group.get(i).record, window));
                } catch (RuntimeException e) {
                    unconverted[i] = wrap(e);
                }
            }
            Exception error = null;
            long baseOffset = -1;
            if (!messages.isEmpty()) {
                try {
                    Partitioning partitioning = tp.partition() == RecordMetadata.UNKNOWN_PARTITION
                            ? Partitioning.balanced()
                            : Partitioning.partitionId((long) tp.partition());
                    TopicId topicId = TopicId.of(tp.topic());
                    long started = System.nanoTime();
                    SendMessagesResponse response = connection.call(
                            c -> c.messages().sendMessages(connection.streamId(), topicId, partitioning, messages));
                    latencySensor.record((System.nanoTime() - started) / 1_000_000.0);
                    if (response != null && !response.confirmations().isEmpty()) {
                        SendConfirmation confirmation = response.confirmations().get(0);
                        baseOffset = confirmation.baseOffset().longValue();
                        tp = new TopicPartition(
                                tp.topic(), confirmation.partitionId().intValue());
                    }
                } catch (Exception e) {
                    error = e;
                }
            }
            long bytes = 0;
            int index = 0;
            for (int i = 0; i < group.size(); i++) {
                Pending p = group.get(i);
                if (unconverted[i] != null) {
                    notify(p, unknownOffset(requested, p), unconverted[i]);
                } else {
                    RecordMetadata metadata =
                            baseOffset < 0 ? unknownOffset(tp, p) : metadata(tp, p, baseOffset, index);
                    notify(p, metadata, error);
                    index++;
                }
                bytes += p.size;
            }
            // Counted and released once per request, not once per record.
            int failed = group.size() - messages.size();
            if (failed > 0) {
                errorSensor.record(failed);
            }
            if (!messages.isEmpty()) {
                (error == null ? sendSensor : errorSensor).record(messages.size());
            }
            release(group.size(), bytes);
        }
    }

    private static RecordMetadata metadata(TopicPartition tp, Pending p, long baseOffset, int index) {
        byte[] key = p.record.key();
        byte[] value = p.record.value();
        return new RecordMetadata(
                tp,
                baseOffset,
                index,
                p.record.timestamp(),
                key == null ? -1 : key.length,
                value == null ? -1 : value.length);
    }

    private static RecordMetadata unknownOffset(TopicPartition tp, Pending p) {
        return metadata(tp, p, -1, -1);
    }

    private void complete(Pending p, RecordMetadata metadata, Exception error) {
        notify(p, metadata, error);
        (error == null ? sendSensor : errorSensor).record(1);
        release(1, p.size);
    }

    /** Callers get Kafka exceptions, as from KafkaProducer. */
    private static Exception wrap(Exception e) {
        return e instanceof KafkaException ? e : new KafkaException(e.getMessage(), e);
    }

    /** Completes the future and runs the callback, without touching the buffer accounting. */
    private static void notify(Pending p, RecordMetadata metadata, Exception error) {
        try {
            if (error == null) {
                p.future.complete(metadata);
            } else {
                p.future.completeExceptionally(error);
            }
            if (p.callback != null) {
                p.callback.onCompletion(metadata, error);
            }
        } catch (RuntimeException e) {
            // A failing user callback must not stop the sender thread; Kafka logs and carries on.
        }
    }

    private void release(int records, long bytes) {
        synchronized (inFlightLock) {
            inFlight -= records;
            bufferedBytes -= bytes;
            inFlightLock.notifyAll();
        }
    }

    @Override
    public void flush() {
        if (Thread.currentThread() == sender) {
            throw new KafkaException(
                    "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.");
        }
        synchronized (inFlightLock) {
            while (inFlight > 0) {
                try {
                    inFlightLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptException(e);
                }
            }
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return connection.partitionsFor(topic, autoCreateTopics);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return metrics.all();
    }

    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Sends what is queued for up to the timeout, then fails the rest, as {@code KafkaProducer}
     * does. Called from a {@link Callback}, it ignores the timeout and returns at once, and the
     * sender thread it runs on sends everything still queued, then closes the connection itself.
     * {@code KafkaProducer} instead treats a close from a callback as a zero timeout and fails the
     * queued records.
     */
    @Override
    public void close(Duration timeout) {
        synchronized (inFlightLock) {
            if (closed) {
                return;
            }
            closed = true;
            inFlightLock.notifyAll(); // Release any send() waiting for buffer space.
        }
        if (Thread.currentThread() == sender) {
            closeOnSenderExit = true;
            return;
        }
        try {
            sender.join(Math.max(1, Math.min(timeout.toMillis(), Long.MAX_VALUE / 2)));
            if (sender.isAlive()) {
                // The sender fails the queued records on this interrupt and exits once the request
                // in flight, if any, is answered or times out.
                sender.interrupt();
                sender.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeResources();
        }
    }

    private void closeResources() {
        Utils.closeQuietly(keySerializer, "key serializer");
        Utils.closeQuietly(valueSerializer, "value serializer");
        connection.close();
        metrics.close();
    }

    @Override
    public void initTransactions() {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public void beginTransaction() {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public void sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    /** From the kafka-clients 3.x interface, removed in 4.0. */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public void commitTransaction() {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public void abortTransaction() {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {}

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {}

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new IllegalStateException("Client telemetry is not available with Iggy");
    }

    /** What KafkaProducer throws from send() when close() runs while the send waits for buffer space. */
    private static final class ClosedWhileSending extends KafkaException {
        ClosedWhileSending() {
            super("Producer closed while send in progress");
        }
    }

    private record Pending(
            TopicPartition partition,
            KafkaRecord record,
            int size,
            CompletableFuture<RecordMetadata> future,
            Callback callback) {}
}
