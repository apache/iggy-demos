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

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.Topic;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * A {@link org.apache.kafka.clients.consumer.Consumer} that reads from Apache Iggy.
 *
 * <p>Use it in place of {@code KafkaConsumer}. {@code group.id}, {@code enable.auto.commit},
 * {@code auto.commit.interval.ms}, {@code auto.offset.reset} and {@code max.poll.records} keep their
 * Kafka meaning.
 *
 * <p>{@link #subscribe} joins an Iggy consumer group of the same name on each topic, and Iggy
 * decides which partitions this member owns. Assignment changes are picked up during {@link #poll}
 * and reported to a {@link ConsumerRebalanceListener} if one was given. {@link #assign} reads the
 * named partitions directly without joining a group.
 *
 * <p>Committed offsets mean the same as in Kafka (the next offset to read). Iggy stores the last
 * consumed offset, so the conversion happens here. Like {@code KafkaConsumer}, this class is not
 * thread safe; only {@link #wakeup} may be called from another thread.
 */
public class IggyKafkaConsumer<K, V> implements org.apache.kafka.clients.consumer.Consumer<K, V> {

    private static final long NO_THREAD = -1L;
    /** Iggy's ConsumerGroupPartitionNotOwned. */
    private static final int PARTITION_NOT_OWNED = 5009;
    /**
     * The partition id in an empty poll reply that tells a group member it no longer owns the
     * partition and must sync its assignment again (u32::MAX on the server).
     */
    private static final long RESYNC_PARTITION = 0xFFFF_FFFFL;

    private static final String FETCH_GROUP = "consumer-fetch-manager-metrics";
    private static final String COORDINATOR_GROUP = "consumer-coordinator-metrics";
    private static final Consumer PROBE = Consumer.of(0L);

    private final IggyConnection connection;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final String groupId;
    private final boolean autoCommit;
    private final long autoCommitIntervalMs;
    private final String autoOffsetReset;
    private final int maxPollRecords;
    private final int fetchMaxRecords;
    private final long maxPartitionFetchBytes;
    private final long assignmentRefreshMs;
    private final long pollIdleMs;
    private final boolean autoCreateTopics;
    private final ClientMetrics metrics;
    private final Sensor consumedSensor;
    private final Sensor bytesSensor;
    private final Sensor fetchLatencySensor;
    private final Sensor fetchSensor;
    private final Sensor commitSensor;

    private final Set<String> subscription = new LinkedHashSet<>();
    private Pattern pattern;
    private ConsumerRebalanceListener listener;
    private boolean manualAssignment;
    private final Map<TopicPartition, PartitionState> assignment = new LinkedHashMap<>();
    private final Set<String> joinedTopics = new HashSet<>();
    private long nextAssignmentRefresh;
    /** Whether the listener heard of the assignment since the subscription last changed. */
    private boolean assignmentAnnounced;

    private long nextAutoCommit;
    private int fetchCursor;

    private final AtomicBoolean wakeup = new AtomicBoolean();
    private final AtomicLong currentThread = new AtomicLong(NO_THREAD);
    private int refCount;
    private volatile boolean closed;

    public IggyKafkaConsumer(Properties properties) {
        this(IggyKafkaConfig.toMap(properties), null, null);
    }

    public IggyKafkaConsumer(
            Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(IggyKafkaConfig.toMap(properties), keyDeserializer, valueDeserializer);
    }

    public IggyKafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public IggyKafkaConsumer(
            Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        IggyKafkaConfig config = new IggyKafkaConfig(configs);
        this.keyDeserializer =
                keyDeserializer != null ? keyDeserializer : Plugins.create(config, "key.deserializer", true);
        this.valueDeserializer =
                valueDeserializer != null ? valueDeserializer : Plugins.create(config, "value.deserializer", false);
        String group = config.string("group.id", null);
        this.groupId = group == null || group.isEmpty() ? null : group;
        this.autoCommit = groupId != null && config.bool("enable.auto.commit", true);
        this.autoCommitIntervalMs = config.longValue("auto.commit.interval.ms", 5000);
        this.autoOffsetReset = config.string("auto.offset.reset", "latest").toLowerCase();
        this.maxPollRecords = config.intValue("max.poll.records", 500);
        this.fetchMaxRecords = config.intValue("iggy.fetch.max.records", 5000);
        this.maxPartitionFetchBytes = config.longValue("max.partition.fetch.bytes", 1024 * 1024L);
        this.assignmentRefreshMs = config.longValue("iggy.assignment.refresh.ms", 1000);
        this.pollIdleMs = Math.max(0, config.longValue("iggy.poll.idle.ms", 20));
        this.autoCreateTopics = config.bool("allow.auto.create.topics", true) && config.autoCreateTopics();
        if (!List.of("earliest", "latest", "none").contains(autoOffsetReset)) {
            throw new org.apache.kafka.common.config.ConfigException(
                    "auto.offset.reset", autoOffsetReset, "must be earliest, latest or none");
        }
        this.connection = new IggyConnection(config);
        this.metrics = new ClientMetrics(config.string("client.id", "iggy-consumer"));
        this.consumedSensor = metrics.totalAndRate(FETCH_GROUP, "records-consumed", "records consumed");
        this.bytesSensor = metrics.totalAndRate(FETCH_GROUP, "bytes-consumed", "bytes fetched");
        this.fetchLatencySensor = metrics.average(FETCH_GROUP, "fetch-latency", "fetch request latency in ms");
        this.fetchSensor = metrics.totalAndRate(FETCH_GROUP, "fetch", "fetch requests");
        this.commitSensor = metrics.totalAndRate(COORDINATOR_GROUP, "commit", "offset commits");
        metrics.gauge(COORDINATOR_GROUP, "assigned-partitions", "Partitions currently assigned", assignment::size);
    }

    // ---- subscription and assignment ----

    @Override
    public Set<TopicPartition> assignment() {
        acquire();
        try {
            return Set.copyOf(assignment.keySet());
        } finally {
            release();
        }
    }

    @Override
    public Set<String> subscription() {
        acquire();
        try {
            return Set.copyOf(subscription);
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, null);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        acquire();
        try {
            requireGroup();
            if (manualAssignment) {
                throw new IllegalStateException("Subscription to topics and manual assignment are mutually exclusive");
            }
            if (topics.isEmpty()) {
                unsubscribeInternal();
                return;
            }
            Set<String> wanted = new LinkedHashSet<>(topics);
            for (String topic : wanted) {
                if (topic == null || topic.isBlank()) {
                    throw new IllegalArgumentException(
                            "Topic collection to subscribe to cannot contain null or empty topic");
                }
            }
            pattern = null;
            changeSubscription(wanted, callback);
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, null);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        acquire();
        try {
            requireGroup();
            if (manualAssignment) {
                throw new IllegalStateException("Subscription to topics and manual assignment are mutually exclusive");
            }
            this.pattern = pattern;
            changeSubscription(matchingTopics(), callback);
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        throw IggyKafkaConfig.unsupported("Broker-side subscription patterns");
    }

    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        throw IggyKafkaConfig.unsupported("Broker-side subscription patterns");
    }

    private void changeSubscription(Set<String> topics, ConsumerRebalanceListener callback) {
        Set<String> dropped = new HashSet<>(subscription);
        dropped.removeAll(topics);
        revoke(assignment.keySet().stream()
                .filter(tp -> dropped.contains(tp.topic()))
                .toList());
        for (String topic : dropped) {
            leaveGroup(topic);
        }
        subscription.clear();
        subscription.addAll(topics);
        listener = callback;
        nextAssignmentRefresh = 0;
        assignmentAnnounced = false;
    }

    private Set<String> matchingTopics() {
        Set<String> topics = new LinkedHashSet<>();
        for (Topic topic : connection.topics()) {
            if (pattern.matcher(topic.name()).matches()) {
                topics.add(topic.name());
            }
        }
        return topics;
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquire();
        try {
            if (!subscription.isEmpty() || pattern != null) {
                throw new IllegalStateException("Subscription to topics and manual assignment are mutually exclusive");
            }
            if (partitions.isEmpty()) {
                unsubscribeInternal();
                return;
            }
            Set<TopicPartition> wanted = new LinkedHashSet<>(partitions);
            if (autoCommit) {
                commit(offsetsToCommit(assignment.keySet().stream()
                        .filter(tp -> !wanted.contains(tp))
                        .toList()));
            }
            for (TopicPartition tp : List.copyOf(assignment.keySet())) {
                if (!wanted.contains(tp)) {
                    drop(tp);
                }
            }
            for (TopicPartition tp : wanted) {
                assignment.computeIfAbsent(tp, this::newState);
            }
            manualAssignment = true;
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe() {
        acquire();
        try {
            unsubscribeInternal();
        } finally {
            release();
        }
    }

    private void unsubscribeInternal() {
        revoke(List.copyOf(assignment.keySet()));
        for (String topic : List.copyOf(joinedTopics)) {
            leaveGroup(topic);
        }
        subscription.clear();
        pattern = null;
        listener = null;
        manualAssignment = false;
    }

    /** Asks Iggy which partitions this member owns and applies any change. */
    private void refreshAssignment() {
        if (manualAssignment || (subscription.isEmpty() && pattern == null)) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now < nextAssignmentRefresh) {
            return;
        }
        nextAssignmentRefresh = now + assignmentRefreshMs;
        if (pattern != null) {
            Set<String> topics = matchingTopics();
            if (!topics.equals(subscription)) {
                changeSubscription(topics, listener);
            }
        }

        Set<TopicPartition> owned = new LinkedHashSet<>();
        for (String topic : subscription) {
            try {
                if (!joinGroup(topic)) {
                    continue;
                }
                Optional<ConsumerGroupAssignment> sync = connection.call(c -> c.consumerGroups()
                        .syncConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId)));
                sync.ifPresent(a -> a.partitions().forEach(p -> owned.add(new TopicPartition(topic, p.intValue()))));
            } catch (KafkaException e) {
                if (!IggyConnection.isGone(e)) {
                    throw e;
                }
                // Deleted since the subscription was resolved: it owns nothing now.
                joinedTopics.remove(topic);
            }
        }

        List<TopicPartition> revoked = new ArrayList<>();
        assignment.forEach((tp, state) -> {
            if (owned.contains(tp)) {
                state.revoking = false;
            } else if (state.buffer.isEmpty() || state.paused) {
                revoked.add(tp);
            } else {
                // Iggy hands the partition over once the commit reaches the last offset it served,
                // so poll returns what is buffered first, and the partition is revoked after that.
                state.revoking = true;
            }
        });
        revoke(revoked);
        List<TopicPartition> added =
                owned.stream().filter(tp -> !assignment.containsKey(tp)).toList();
        for (TopicPartition tp : added) {
            assignment.put(tp, newState(tp));
        }
        if (listener != null && (!added.isEmpty() || !revoked.isEmpty() || !assignmentAnnounced)) {
            listener.onPartitionsAssigned(added);
        }
        assignmentAnnounced = true;
    }

    private boolean joinGroup(String topic) {
        if (joinedTopics.contains(topic)) {
            return true;
        }
        try {
            connection.describe(topic, autoCreateTopics);
        } catch (UnknownTopicOrPartitionException e) {
            return false; // Kafka keeps waiting for a subscribed topic to appear.
        }
        connection.ensureGroup(topic, groupId);
        connection.call(c -> {
            c.consumerGroups().joinConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId));
            return null;
        });
        joinedTopics.add(topic);
        return true;
    }

    private void leaveGroup(String topic) {
        if (!joinedTopics.remove(topic)) {
            return;
        }
        try {
            connection.call(c -> {
                c.consumerGroups().leaveConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId));
                return null;
            });
        } catch (KafkaException e) {
            // Leaving is best effort; the server drops the member when the connection closes.
        }
    }

    private void revoke(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        if (autoCommit) {
            try {
                commit(offsetsToCommit(partitions));
            } catch (KafkaException e) {
                // Iggy may already have handed the partition to another member; the next owner
                // re-reads from the last commit, as after a failed Kafka commit.
            }
        }
        if (listener != null) {
            listener.onPartitionsRevoked(partitions);
        }
        partitions.forEach(this::drop);
    }

    // ---- polling ----

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        acquire();
        try {
            if (!manualAssignment && subscription.isEmpty() && pattern == null) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }
            long deadline = deadline(timeout);
            do {
                checkWakeup();
                refreshAssignment();
                maybeAutoCommit();
                ConsumerRecords<K, V> records = fetch();
                if (!records.isEmpty()) {
                    return records;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    pause(Math.min(remaining, pollIdleMs));
                }
            } while (System.currentTimeMillis() < deadline);
            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }

    /** Now plus the timeout, without overflowing for the Long.MAX_VALUE timeouts Kafka callers pass. */
    private static long deadline(Duration timeout) {
        long now = System.currentTimeMillis();
        long millis = Math.max(0, timeout.toMillis());
        return millis > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + millis;
    }

    private ConsumerRecords<K, V> fetch() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new LinkedHashMap<>();
        Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
        List<TopicPartition> partitions = new ArrayList<>(assignment.keySet());
        int remaining = maxPollRecords;
        for (int i = 0; i < partitions.size() && remaining > 0; i++) {
            TopicPartition tp = partitions.get((fetchCursor + i) % partitions.size());
            PartitionState state = assignment.get(tp);
            if (state.paused) {
                continue;
            }
            long position = position(tp, state);
            if (state.revoking) {
                if (state.buffer.isEmpty()) {
                    nextAssignmentRefresh = 0;
                    continue;
                }
            } else if (state.buffer.isEmpty() && !fill(tp, state, position)) {
                continue;
            }
            List<ConsumerRecord<K, V>> records = new ArrayList<>();
            RecordDeserializationException failure = null;
            while (!state.buffer.isEmpty() && records.size() < remaining) {
                Message message = state.buffer.peekFirst();
                long offset = message.header().offset().longValue();
                try {
                    records.add(toRecord(tp, offset, message));
                } catch (RuntimeException e) {
                    // As in Kafka: the position stays on the bad record, and the caller can seek past it.
                    failure = new RecordDeserializationException(
                            tp, offset, "Error deserializing record at offset " + offset + " of " + tp, e);
                    break;
                }
                state.buffer.pollFirst();
                state.position = offset + 1;
            }
            if (state.revoking && state.buffer.isEmpty()) {
                nextAssignmentRefresh = 0;
            }
            if (!records.isEmpty()) {
                result.put(tp, records);
                nextOffsets.put(tp, new OffsetAndMetadata(state.position));
                remaining -= records.size();
                consumedSensor.record(records.size());
            }
            if (failure != null) {
                if (result.isEmpty()) {
                    throw failure;
                }
                break; // Hand over what was read; the next poll reaches the bad record and throws.
            }
        }
        if (!partitions.isEmpty()) {
            fetchCursor = (fetchCursor + 1) % partitions.size();
        }
        return result.isEmpty() ? ConsumerRecords.empty() : records(result, nextOffsets);
    }

    /**
     * Fetches the next chunk of a partition into its buffer, as Kafka does: requests are sized by
     * {@code iggy.fetch.max.records}, and {@code max.poll.records} only limits what one poll returns.
     * Iggy counts a request in messages, not bytes, so once a partition's mean message size is
     * known the count is cut to keep a request near {@code max.partition.fetch.bytes}.
     */
    private boolean fill(TopicPartition tp, PartitionState state, long position) {
        PolledMessages polled;
        long count = fetchCount(state);
        long started = System.nanoTime();
        try {
            polled = connection.call(c -> c.messages()
                    .pollMessages(
                            connection.streamId(),
                            TopicId.of(tp.topic()),
                            Optional.of((long) tp.partition()),
                            consumerFor(),
                            PollingStrategy.offset(BigInteger.valueOf(position)),
                            count,
                            false));
        } catch (KafkaException e) {
            if (isServerError(e, PARTITION_NOT_OWNED) || IggyConnection.isGone(e)) {
                // Iggy is handing this partition to another member, or its topic was deleted;
                // pick that up at the next refresh.
                nextAssignmentRefresh = 0;
                return false;
            }
            throw e;
        }
        fetchLatencySensor.record((System.nanoTime() - started) / 1_000_000.0);
        fetchSensor.record(1);
        if (polled.messages().isEmpty() && polled.partitionId() == RESYNC_PARTITION) {
            // Iggy 0.9 answers a group poll for a partition this member no longer owns with an
            // empty reply carrying this partition id, not with an error.
            nextAssignmentRefresh = 0;
            return false;
        }
        // Iggy reports the partition's last offset with every poll. An empty partition also reports
        // 0, so 0 only counts as a real offset when a message came back with it.
        BigInteger last = polled.currentOffset();
        state.logEndOffset = last.signum() > 0 || !polled.messages().isEmpty() ? last.longValue() + 1 : 0L;
        long bytes = 0;
        for (Message message : polled.messages()) {
            if (message.header().offset().longValue() >= position) {
                state.buffer.addLast(message);
                bytes += message.payload().length;
            }
        }
        if (bytes > 0) {
            bytesSensor.record(bytes);
            state.meanMessageBytes = Math.max(1, bytes / state.buffer.size());
        }
        return !state.buffer.isEmpty();
    }

    /** Messages to ask for: the configured count, cut to about {@code max.partition.fetch.bytes}. */
    private long fetchCount(PartitionState state) {
        if (state.meanMessageBytes == 0) {
            return fetchMaxRecords;
        }
        return Math.max(1, Math.min(fetchMaxRecords, maxPartitionFetchBytes / state.meanMessageBytes));
    }

    /**
     * The constructor that carries next offsets only exists from kafka-clients 4.0. Looked up at
     * runtime so the same jar also works with a 3.x client on the classpath.
     */
    private static final java.lang.reflect.Constructor<?> RECORDS_WITH_NEXT_OFFSETS = recordsWithNextOffsets();

    private static java.lang.reflect.Constructor<?> recordsWithNextOffsets() {
        try {
            return ConsumerRecords.class.getConstructor(Map.class, Map.class);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> ConsumerRecords<K, V> records(
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records,
            Map<TopicPartition, OffsetAndMetadata> nextOffsets) {
        if (RECORDS_WITH_NEXT_OFFSETS != null) {
            try {
                return (ConsumerRecords<K, V>) RECORDS_WITH_NEXT_OFFSETS.newInstance(records, nextOffsets);
            } catch (ReflectiveOperationException e) {
                // Fall through to the constructor every version has.
            }
        }
        return new ConsumerRecords<>(records);
    }

    private ConsumerRecord<K, V> toRecord(TopicPartition tp, long offset, Message message) {
        RecordCodec.Decoded decoded = RecordCodec.fromMessage(message);
        byte[] keyBytes = decoded.key();
        byte[] valueBytes = decoded.value();
        var headers = decoded.headers();
        K key = keyDeserializer.deserialize(tp.topic(), headers, keyBytes);
        V value = valueDeserializer.deserialize(tp.topic(), headers, valueBytes);
        return new ConsumerRecord<>(
                tp.topic(),
                tp.partition(),
                offset,
                decoded.timestamp(),
                TimestampType.CREATE_TIME,
                keyBytes == null ? -1 : keyBytes.length,
                valueBytes == null ? -1 : valueBytes.length,
                key,
                value,
                headers,
                Optional.empty());
    }

    /** The current position, resolving it from the committed offset or the reset policy if unset. */
    private long position(TopicPartition tp, PartitionState state) {
        if (state.position == null) {
            OffsetAndMetadata committed = groupId == null ? null : connection.committed(tp, offsetOwner());
            if (committed != null) {
                state.position = committed.offset();
                state.committed = committed.offset();
            } else if (autoOffsetReset.equals("earliest")) {
                state.position = beginningOffset(tp);
            } else if (autoOffsetReset.equals("latest")) {
                state.position = endOffset(tp);
            } else {
                throw new NoOffsetForPartitionException(tp);
            }
        }
        return state.position;
    }

    /**
     * Iggy only accepts group offsets from the member that owns the partition, so manually assigned
     * partitions keep their offsets under a standalone consumer named after {@code group.id}. Those
     * offsets are separate from the ones the same group commits through {@link #subscribe}.
     */
    private Consumer offsetOwner() {
        return manualAssignment ? Consumer.of(ConsumerId.of(groupId)) : Consumer.group(ConsumerId.of(groupId));
    }

    /**
     * Iggy only serves group polls to members, so manually assigned partitions are read as a plain
     * consumer. Their offsets are still committed under the group.
     */
    private Consumer consumerFor() {
        return groupId == null || manualAssignment ? PROBE : Consumer.group(ConsumerId.of(groupId));
    }

    /** From the kafka-clients 3.x interface, removed in 4.0. */
    @Deprecated
    public ConsumerRecords<K, V> poll(long timeoutMs) {
        return poll(Duration.ofMillis(timeoutMs));
    }

    // ---- offsets ----

    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Duration timeout) {
        acquire();
        try {
            commit(offsetsToCommit(assignment.keySet()));
        } finally {
            release();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        acquire();
        try {
            commit(offsets);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        acquire();
        try {
            commitAsync(offsetsToCommit(assignment.keySet()), callback);
        } finally {
            release();
        }
    }

    /** Commits immediately; Iggy offset stores are cheap, so there is no background queue. */
    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquire();
        try {
            Exception error = null;
            try {
                commit(offsets);
            } catch (RuntimeException e) {
                error = e;
            }
            if (callback != null) {
                callback.onComplete(offsets, error);
            }
        } finally {
            release();
        }
    }

    /** The positions that differ from the last commit, so a seek backwards is committed too. */
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit(Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            PartitionState state = assignment.get(tp);
            if (state != null && state.position != null && state.position != state.committed) {
                offsets.put(tp, new OffsetAndMetadata(state.position));
            }
        }
        return offsets;
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) {
            return;
        }
        requireGroup();
        // Kafka lets a consumer commit for a group it has not joined while that group has no
        // members. Iggy only takes group offsets from a member, so join for the duration of the
        // commit and leave again.
        Set<String> borrowed = new HashSet<>();
        try {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long next = entry.getValue().offset();
                if (next <= 0) {
                    continue; // Iggy stores the last consumed offset; "nothing consumed" has no encoding.
                }
                if (!manualAssignment && !joinedTopics.contains(tp.topic()) && borrowed.add(tp.topic())) {
                    joinEmptyGroup(tp.topic());
                }
                Consumer owner = offsetOwner();
                connection.call(c -> {
                    c.consumerOffsets()
                            .storeConsumerOffset(
                                    connection.streamId(),
                                    TopicId.of(tp.topic()),
                                    Optional.of((long) tp.partition()),
                                    owner,
                                    BigInteger.valueOf(next - 1));
                    return null;
                });
                PartitionState state = assignment.get(tp);
                if (state != null) {
                    state.committed = next;
                }
            }
        } finally {
            for (String topic : borrowed) {
                try {
                    connection.call(c -> {
                        c.consumerGroups()
                                .leaveConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId));
                        return null;
                    });
                } catch (KafkaException e) {
                    // Best effort; the server drops the member when the connection closes.
                }
            }
        }
        commitSensor.record(1);
        nextAutoCommit = System.currentTimeMillis() + autoCommitIntervalMs;
    }

    /** Joins the group on a topic this consumer is not subscribed to, refusing if it has members, as Kafka does. */
    private void joinEmptyGroup(String topic) {
        connection.describe(topic, false);
        connection.ensureGroup(topic, groupId);
        TopicId topicId = TopicId.of(topic);
        connection.call(c -> {
            var group = c.consumerGroups().getConsumerGroup(connection.streamId(), topicId, ConsumerId.of(groupId));
            if (group.isPresent() && group.get().membersCount() > 0) {
                throw new CommitFailedException("Group " + groupId + " has active members on " + topic
                        + "; only a member can commit offsets for it");
            }
            c.consumerGroups().joinConsumerGroup(connection.streamId(), topicId, ConsumerId.of(groupId));
            return null;
        });
    }

    /** As in Kafka, a failed automatic commit does not fail poll; the next one retries. */
    private void maybeAutoCommit() {
        if (autoCommit && System.currentTimeMillis() >= nextAutoCommit) {
            try {
                commit(offsetsToCommit(assignment.keySet()));
            } catch (KafkaException e) {
                // A partition moved or its topic was deleted; the next refresh catches up.
            }
            nextAutoCommit = System.currentTimeMillis() + autoCommitIntervalMs;
        }
    }

    private static boolean isServerError(KafkaException e, int code) {
        return e.getCause() instanceof IggyServerException server && server.getRawErrorCode() == code;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        acquire();
        try {
            requireGroup();
            Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
            for (TopicPartition tp : partitions) {
                result.put(tp, connection.committed(tp, offsetOwner()));
            }
            return result;
        } finally {
            release();
        }
    }

    /** From the kafka-clients 3.x interface, removed in 4.0. */
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(Set.of(partition)).get(partition);
    }

    /** From the kafka-clients 3.x interface, removed in 4.0. */
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return committed(Set.of(partition), timeout).get(partition);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }
        acquire();
        try {
            state(partition).moveTo(offset);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        seek(partition, offsetAndMetadata.offset());
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        acquire();
        try {
            for (TopicPartition tp : partitions.isEmpty() ? assignment.keySet() : partitions) {
                state(tp).moveTo(beginningOffset(tp));
            }
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        acquire();
        try {
            for (TopicPartition tp : partitions.isEmpty() ? assignment.keySet() : partitions) {
                state(tp).moveTo(endOffset(tp));
            }
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        acquire();
        try {
            return position(partition, state(partition));
        } finally {
            release();
        }
    }

    private PartitionState state(TopicPartition tp) {
        PartitionState state = assignment.get(tp);
        if (state == null) {
            throw new IllegalStateException("No current assignment for partition " + tp);
        }
        return state;
    }

    private long beginningOffset(TopicPartition tp) {
        Message first = connection.probe(tp, PollingStrategy.first());
        return first == null ? 0 : first.header().offset().longValue();
    }

    private long endOffset(TopicPartition tp) {
        Message last = connection.probe(tp, PollingStrategy.last());
        return last == null ? 0 : last.header().offset().longValue() + 1;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquire();
        try {
            Map<TopicPartition, Long> result = new HashMap<>();
            partitions.forEach(tp -> result.put(tp, beginningOffset(tp)));
            return result;
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquire();
        try {
            Map<TopicPartition, Long> result = new HashMap<>();
            partitions.forEach(tp -> result.put(tp, endOffset(tp)));
            return result;
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquire();
        try {
            Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                TopicPartition tp = entry.getKey();
                BigInteger micros = BigInteger.valueOf(entry.getValue()).multiply(BigInteger.valueOf(1000));
                Message m = connection.probe(tp, PollingStrategy.timestamp(micros));
                result.put(
                        tp,
                        m == null
                                ? null
                                : new OffsetAndTimestamp(
                                        m.header().offset().longValue(), RecordCodec.searchTimestamp(m)));
            }
            return result;
        } finally {
            release();
        }
    }

    /** Records between the consumer's position and the partition's end, as of the last fetch from it. */
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquire();
        try {
            PartitionState state = assignment.get(topicPartition);
            if (state == null || state.position == null || state.logEndOffset == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(Math.max(0, state.logEndOffset - state.position));
        } finally {
            release();
        }
    }

    // ---- flow control ----

    @Override
    public Set<TopicPartition> paused() {
        acquire();
        try {
            Set<TopicPartition> result = new HashSet<>();
            assignment.forEach((tp, s) -> {
                if (s.paused) {
                    result.add(tp);
                }
            });
            return result;
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquire();
        try {
            partitions.forEach(tp -> state(tp).paused = true);
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquire();
        try {
            partitions.forEach(tp -> state(tp).paused = false);
        } finally {
            release();
        }
    }

    // ---- metadata ----

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquire();
        try {
            return connection.partitionsFor(topic, false);
        } catch (UnknownTopicOrPartitionException e) {
            return List.of();
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquire();
        try {
            Map<String, List<PartitionInfo>> result = new HashMap<>();
            for (Topic topic : connection.topics()) {
                result.put(topic.name(), IggyConnection.partitionInfos(topic.name(), topic.partitionsCount()));
            }
            return result;
        } finally {
            release();
        }
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        requireGroup();
        return new ConsumerGroupMetadata(groupId);
    }

    @Override
    public void enforceRebalance() {
        nextAssignmentRefresh = 0;
    }

    @Override
    public void enforceRebalance(String reason) {
        enforceRebalance();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return metrics.all();
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {}

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {}

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new IllegalStateException("Client telemetry is not available with Iggy");
    }

    // ---- lifecycle ----

    @Override
    public void wakeup() {
        wakeup.set(true);
    }

    @Override
    public void close() {
        close(Duration.ofSeconds(30));
    }

    @Override
    public void close(CloseOptions options) {
        close(options.timeout().orElse(Duration.ofSeconds(30)));
    }

    @Override
    public void close(Duration timeout) {
        acquire(true);
        try {
            if (closed) {
                return;
            }
            try {
                if (autoCommit) {
                    try {
                        commit(offsetsToCommit(assignment.keySet()));
                    } catch (KafkaException e) {
                        // As KafkaConsumer: a failed final auto-commit does not fail close().
                    }
                }
                if (listener != null && !assignment.isEmpty()) {
                    listener.onPartitionsRevoked(List.copyOf(assignment.keySet()));
                }
                for (String topic : List.copyOf(joinedTopics)) {
                    leaveGroup(topic);
                }
            } finally {
                closed = true;
                List.copyOf(assignment.keySet()).forEach(this::drop);
                Utils.closeQuietly(keyDeserializer, "key deserializer");
                Utils.closeQuietly(valueDeserializer, "value deserializer");
                connection.close();
                metrics.close();
            }
        } finally {
            release();
        }
    }

    // ---- helpers ----

    private void requireGroup() {
        if (groupId == null) {
            throw new InvalidGroupIdException(
                    "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.");
        }
    }

    private void checkWakeup() {
        if (wakeup.getAndSet(false)) {
            throw new WakeupException();
        }
    }

    private void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptException(e);
        }
    }

    private void acquire() {
        acquire(false);
    }

    /** Same single-thread guard as KafkaConsumer; close() is allowed after the consumer is closed. */
    private void acquire(boolean allowClosed) {
        if (closed && !allowClosed) {
            throw new IllegalStateException("This consumer has already been closed.");
        }
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_THREAD, threadId)) {
            throw new java.util.ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        }
        refCount++;
    }

    private void release() {
        if (--refCount == 0) {
            currentThread.set(NO_THREAD);
        }
    }

    /** Tracks a partition and publishes Kafka's per-partition {@code records-lag} metric for it. */
    private PartitionState newState(TopicPartition tp) {
        PartitionState state = new PartitionState();
        // Kafka replaces dots in the topic tag, and tools such as Flink look the metric up that way.
        Map<String, String> tags =
                Map.of("topic", tp.topic().replace('.', '_'), "partition", String.valueOf(tp.partition()));
        state.lagMetric = metrics.gauge(FETCH_GROUP, "records-lag", "Records behind the partition end", tags, () -> {
            if (state.position == null || state.logEndOffset == null) {
                return 0.0;
            }
            return (double) Math.max(0, state.logEndOffset - state.position);
        });
        return state;
    }

    private void drop(TopicPartition tp) {
        PartitionState state = assignment.remove(tp);
        if (state != null && state.lagMetric != null) {
            metrics.remove(state.lagMetric);
        }
    }

    private static final class PartitionState {
        Long position;
        long committed;
        boolean paused;
        /** Iggy moved the partition away; poll returns what is buffered, then it is revoked. */
        boolean revoking;
        /** One past the partition's last offset, as reported by the most recent fetch. */
        Long logEndOffset;
        /** Mean payload size of the last fetch, or 0 before the first one; sizes the next request. */
        long meanMessageBytes;

        org.apache.kafka.common.MetricName lagMetric;
        /** Fetched records from {@code position} on, not yet returned by poll. */
        final java.util.ArrayDeque<Message> buffer = new java.util.ArrayDeque<>();

        void moveTo(long offset) {
            position = offset;
            buffer.clear();
        }
    }
}
