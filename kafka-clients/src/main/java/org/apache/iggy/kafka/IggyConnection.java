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

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * One logged-in Iggy TCP client plus the topic metadata the Kafka facades need.
 *
 * <p>Every Kafka topic maps to an Iggy topic of the same name inside a single configured stream.
 * Access to the underlying client is serialised, so the producer's caller threads and its sender
 * thread can share one connection.
 */
final class IggyConnection implements AutoCloseable {

    /**
     * Iggy's errors for a stream, topic, partition, group or member that does not exist. Under a
     * subscription they mean a metadata refresh, as in Kafka, and on an offset read they mean
     * "nothing committed". Any other error is a real failure.
     */
    private static final Set<Integer> GONE = Set.of(1009, 1010, 2010, 2011, 3007, 5000, 5003, 5006);

    /** A plain consumer for reads that touch no group state. */
    private static final Consumer PROBE = Consumer.of(0L);

    private final IggyKafkaConfig config;
    private final IggyTcpClient client;
    private final StreamId streamId;
    private final Map<String, CachedCount> partitionCounts = new ConcurrentHashMap<>();
    private final long metadataMaxAgeMs;
    private volatile boolean streamChecked;

    IggyConnection(IggyKafkaConfig config) {
        this.config = config;
        this.streamId = StreamId.of(config.stream());
        this.metadataMaxAgeMs = config.longValue("metadata.max.age.ms", 300_000);
        Security security = Security.from(config);
        Object rawTimeout = config.raw("request.timeout.ms");
        long requestTimeout = config.longValue("request.timeout.ms", -1);
        if (rawTimeout != null && requestTimeout <= 0) {
            // Checked here, not left to the SDK, so it is not reported as a failed connection.
            throw new ConfigException("request.timeout.ms", rawTimeout, "must be greater than 0 for Iggy");
        }
        try {
            var builder = IggyTcpClient.builder()
                    .host(config.host())
                    .port(config.port())
                    .credentials(security.username(), security.password());
            if (rawTimeout != null) {
                builder.requestTimeout(Duration.ofMillis(requestTimeout));
            }
            if (security.tls()) {
                builder.enableTls();
                if (security.trustedCertificates() != null) {
                    builder.tlsCertificate(security.trustedCertificates());
                }
            }
            this.client = builder.buildAndLogin();
        } catch (RuntimeException e) {
            throw new KafkaException("Failed to connect to Iggy at " + config.host() + ":" + config.port(), e);
        }
    }

    /** Runs one call against the shared client. The blocking client is not shared concurrently. */
    synchronized <T> T call(Function<IggyTcpClient, T> fn) {
        try {
            return fn.apply(client);
        } catch (KafkaException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new KafkaException(e.getMessage(), e);
        }
    }

    /** Whether an error from {@link #call} says the thing addressed does not exist. */
    static boolean isGone(KafkaException e) {
        return e.getCause() instanceof IggyServerException server && GONE.contains(server.getRawErrorCode());
    }

    StreamId streamId() {
        return streamId;
    }

    /**
     * Partition count for a topic, creating the topic first when auto-creation is enabled. Cached
     * for {@code metadata.max.age.ms}, as Kafka does, so partitions added on the server are seen.
     */
    long partitionCount(String topic) {
        CachedCount cached = partitionCounts.get(topic);
        if (cached != null && System.nanoTime() - cached.fetchedAt < metadataMaxAgeMs * 1_000_000L) {
            return cached.count;
        }
        long count = describe(topic, config.autoCreateTopics()).partitionsCount();
        remember(topic, count);
        return count;
    }

    private void remember(String topic, long count) {
        partitionCounts.put(topic, new CachedCount(count, System.nanoTime()));
    }

    private record CachedCount(long count, long fetchedAt) {}

    /** Drops cached metadata so the next lookup sees partitions added on the server. */
    void invalidate(String topic) {
        partitionCounts.remove(topic);
    }

    /**
     * Checks once that the configured stream exists, creating it when {@code create} is set. Fails
     * with {@link UnknownTopicOrPartitionException} when it is missing and may not be created.
     */
    synchronized void ensureStream(boolean create) {
        if (streamChecked) {
            return;
        }
        if (client.streams().getStream(streamId).isEmpty()) {
            if (!create) {
                throw new UnknownTopicOrPartitionException("Iggy stream " + config.stream() + " does not exist");
            }
            try {
                client.streams().createStream(config.stream());
            } catch (RuntimeException e) {
                // Another client may have created it first.
                if (client.streams().getStream(streamId).isEmpty()) {
                    throw new KafkaException("Failed to create Iggy stream " + config.stream(), e);
                }
            }
        }
        streamChecked = true;
    }

    /** The numeric id Iggy gave the configured stream. */
    synchronized long streamNumericId() {
        return client.streams()
                .getStream(streamId)
                .orElseThrow(() ->
                        new UnknownTopicOrPartitionException("Iggy stream " + config.stream() + " does not exist"))
                .id();
    }

    synchronized TopicDetails describe(String topic, boolean create) {
        TopicId topicId = TopicId.of(topic);
        ensureStream(create);
        var existing = client.topics().getTopic(streamId, topicId);
        if (existing.isPresent()) {
            return existing.get();
        }
        if (!create) {
            throw new UnknownTopicOrPartitionException(
                    "Topic " + topic + " not present in Iggy stream " + config.stream());
        }
        try {
            return client.topics()
                    .createTopic(
                            streamId,
                            config.defaultPartitions(),
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            topic);
        } catch (RuntimeException e) {
            return client.topics()
                    .getTopic(streamId, topicId)
                    .orElseThrow(() -> new KafkaException("Failed to create Iggy topic " + topic, e));
        }
    }

    /** The single message a strategy selects, without touching any group state, or null when there is none. */
    Message probe(TopicPartition tp, PollingStrategy strategy) {
        PolledMessages polled = call(c -> c.messages()
                .pollMessages(
                        streamId,
                        TopicId.of(tp.topic()),
                        Optional.of((long) tp.partition()),
                        PROBE,
                        strategy,
                        1L,
                        false));
        return polled.messages().isEmpty() ? null : polled.messages().get(0);
    }

    /**
     * The committed offset as Kafka means it, the next one to read; Iggy stores the last consumed
     * one. Null when the group or topic does not exist, so nothing is committed.
     */
    OffsetAndMetadata committed(TopicPartition tp, Consumer owner) {
        Optional<ConsumerOffsetInfo> info;
        try {
            info = call(c -> c.consumerOffsets()
                    .getConsumerOffset(streamId, TopicId.of(tp.topic()), Optional.of((long) tp.partition()), owner));
        } catch (KafkaException e) {
            if (isGone(e)) {
                return null;
            }
            throw e; // A failed read must not look like "nothing committed" and reset the position.
        }
        return info.map(i -> new OffsetAndMetadata(i.storedOffset().longValue() + 1))
                .orElse(null);
    }

    /** Creates the consumer group on a topic unless it exists, allowing for another client creating it first. */
    void ensureGroup(String topic, String groupId) {
        TopicId topicId = TopicId.of(topic);
        ConsumerId consumerId = ConsumerId.of(groupId);
        call(c -> {
            if (c.consumerGroups()
                    .getConsumerGroup(streamId, topicId, consumerId)
                    .isEmpty()) {
                try {
                    c.consumerGroups().createConsumerGroup(streamId, topicId, groupId);
                } catch (RuntimeException e) {
                    if (c.consumerGroups()
                            .getConsumerGroup(streamId, topicId, consumerId)
                            .isEmpty()) {
                        throw e;
                    }
                }
            }
            return null;
        });
    }

    List<PartitionInfo> partitionsFor(String topic, boolean create) {
        long count = describe(topic, create).partitionsCount();
        remember(topic, count);
        return partitionInfos(topic, count);
    }

    synchronized List<Topic> topics() {
        if (client.streams().getStream(streamId).isEmpty()) {
            return List.of();
        }
        return client.topics().getTopics(streamId);
    }

    static List<PartitionInfo> partitionInfos(String topic, long count) {
        List<PartitionInfo> infos = new ArrayList<>();
        for (int p = 0; p < count; p++) {
            infos.add(new PartitionInfo(topic, p, null, null, null));
        }
        return infos;
    }

    @Override
    public synchronized void close() {
        client.close();
    }
}
