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

import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Ulimit;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the Kafka-shaped clients against a real Iggy server in Docker. */
class IggyKafkaClientsTest {

    private static GenericContainer<?> iggy;
    private static String bootstrap;

    @BeforeAll
    static void startIggy() {
        iggy = new GenericContainer<>(System.getProperty("iggy.image", "apache/iggy:0.9.0"))
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
                .withEnv("IGGY_NODE_ADVERTISED_ADDRESS", "localhost")
                .withEnv("IGGY_SHARDING_CPU_ALLOCATION", "2")
                .withExposedPorts(8090)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withCapAdd(Capability.SYS_NICE)
                        .withSecurityOpts(List.of("seccomp=unconfined"))
                        .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                .waitingFor(Wait.forLogMessage(".*Client listener \\(TCP\\) accepting.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        iggy.start();
        bootstrap = iggy.getHost() + ":" + iggy.getMappedPort(8090);
    }

    @AfterAll
    static void stopIggy() {
        if (iggy != null) {
            iggy.stop();
        }
    }

    private static String topic() {
        return "t-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static Properties producerProps(int partitions) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("iggy.password", "iggy");
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        p.put("iggy.default.partitions", String.valueOf(partitions));
        return p;
    }

    private static Properties consumerProps(String group) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("iggy.password", "iggy");
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("auto.offset.reset", "earliest");
        p.put("iggy.assignment.refresh.ms", "200");
        if (group != null) {
            p.put("group.id", group);
        }
        return p;
    }

    @Test
    void reportsLagFromTheLatestFetch() throws Exception {
        String topic = topic();
        produce(topic, 1, 10);
        Properties props = consumerProps("lag-" + topic);
        props.put("max.poll.records", "4");
        TopicPartition tp = new TopicPartition(topic, 0);
        try (IggyKafkaConsumer<String, String> consumer = new IggyKafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            assertTrue(consumer.currentLag(tp).isEmpty(), "no fetch yet");
            int received = 0;
            while (received < 4) {
                received += consumer.poll(Duration.ofSeconds(1)).count();
            }
            assertEquals(4, received);
            assertEquals(6, consumer.currentLag(tp).getAsLong());
            assertEquals(6.0, metric(consumer, "records-lag", topic, "0"), "records-lag gauge for the partition");
            assertTrue(metric(consumer, "bytes-consumed-total", null, null) > 0, "bytes fetched");
            while (received < 10) {
                received += consumer.poll(Duration.ofSeconds(1)).count();
            }
            assertEquals(0, consumer.currentLag(tp).getAsLong());
            assertEquals(0.0, metric(consumer, "records-lag", topic, "0"));
            consumer.unsubscribe();
            assertTrue(
                    consumer.metrics().keySet().stream().noneMatch(n -> n.name().equals("records-lag")),
                    "gauge removed with the partition");
        }
    }

    /** A consumer-fetch-manager metric by name, optionally for one partition, as Kafka tags it. */
    private static double metric(IggyKafkaConsumer<?, ?> consumer, String name, String topic, String partition) {
        return consumer.metrics().entrySet().stream()
                .filter(e -> e.getKey().group().equals("consumer-fetch-manager-metrics")
                        && e.getKey().name().equals(name))
                .filter(e -> topic == null
                        ? !e.getKey().tags().containsKey("topic")
                        : topic.equals(e.getKey().tags().get("topic"))
                                && partition.equals(e.getKey().tags().get("partition")))
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("metric " + name + " not found"));
    }

    @Test
    void commitsForAnEmptyGroupWithoutJoining() throws Exception {
        String topic = topic();
        String group = "outsider-" + topic;
        produce(topic, 1, 10);
        TopicPartition tp = new TopicPartition(topic, 0);
        try (IggyKafkaConsumer<String, String> outsider = new IggyKafkaConsumer<>(consumerProps(group))) {
            outsider.commitSync(Map.of(tp, new OffsetAndMetadata(4)));
            assertEquals(4, outsider.committed(Set.of(tp)).get(tp).offset());
        }
        try (IggyKafkaConsumer<String, String> member = new IggyKafkaConsumer<>(consumerProps(group));
                IggyKafkaConsumer<String, String> outsider = new IggyKafkaConsumer<>(consumerProps(group))) {
            member.subscribe(List.of(topic));
            assertEquals(
                    4,
                    member.poll(Duration.ofSeconds(5)).iterator().next().offset(),
                    "resumes from the outsider's commit");
            assertThrows(
                    org.apache.kafka.clients.consumer.CommitFailedException.class,
                    () -> outsider.commitSync(Map.of(tp, new OffsetAndMetadata(2))));
        }
    }

    private static void produce(String topic, int partitions, int count) throws Exception {
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(partitions))) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i));
            }
            producer.flush();
        }
    }

    private static List<ConsumerRecord<String, String>> drain(
            org.apache.kafka.clients.consumer.Consumer<String, String> consumer, int expected) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;
        while (all.size() < expected && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(all::add);
        }
        return all;
    }

    @Test
    void roundTripKeepsKeysValuesHeadersAndPartitions() throws Exception {
        String topic = topic();
        int partitions = 3;
        List<RecordMetadata> metadata = new ArrayList<>();
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(partitions))) {
            for (int i = 0; i < 9; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, "value-" + i);
                record.headers().add("trace", ("id-" + i).getBytes(StandardCharsets.UTF_8));
                record.headers().add("empty", new byte[0]);
                record.headers().add("nothing", null);
                metadata.add(producer.send(record).get());
            }
            metadata.add(producer.send(new ProducerRecord<>(topic, "tombstone", null))
                    .get());
        }

        for (int i = 0; i < 9; i++) {
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            assertEquals(
                    Utils.toPositive(Utils.murmur2(key)) % partitions,
                    metadata.get(i).partition(),
                    "keyed records land where Kafka's default partitioner puts them");
            assertTrue(metadata.get(i).hasOffset());
        }

        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(null))) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                tps.add(new TopicPartition(topic, p));
            }
            consumer.assign(tps);
            List<ConsumerRecord<String, String>> records = drain(consumer, 10);
            assertEquals(10, records.size());
            for (ConsumerRecord<String, String> r : records) {
                if (r.key().equals("tombstone")) {
                    assertNull(r.value());
                    continue;
                }
                String n = r.key().substring(4);
                assertEquals("value-" + n, r.value());
                assertArrayEquals(
                        ("id-" + n).getBytes(StandardCharsets.UTF_8),
                        r.headers().lastHeader("trace").value());
                assertArrayEquals(new byte[0], r.headers().lastHeader("empty").value());
                assertNull(r.headers().lastHeader("nothing").value());
                assertTrue(r.timestamp() > 0);
            }
        }
    }

    @Test
    void offsetsMatchProducerMetadataAndSeekWorks() throws Exception {
        String topic = topic();
        TopicPartition tp = new TopicPartition(topic, 0);
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(1))) {
            for (int i = 0; i < 5; i++) {
                assertEquals(
                        i,
                        producer.send(new ProducerRecord<>(topic, 0, null, "v" + i))
                                .get()
                                .offset());
            }
        }
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(null))) {
            consumer.assign(List.of(tp));
            assertEquals(Map.of(tp, 0L), consumer.beginningOffsets(List.of(tp)));
            assertEquals(Map.of(tp, 5L), consumer.endOffsets(List.of(tp)));

            consumer.seek(tp, 3);
            List<ConsumerRecord<String, String>> records = drain(consumer, 2);
            assertEquals(
                    List.of(3L, 4L),
                    records.stream().map(ConsumerRecord::offset).toList());
            assertEquals(5, consumer.position(tp));

            consumer.seekToBeginning(List.of(tp));
            assertEquals(0, consumer.position(tp));
            consumer.seekToEnd(List.of(tp));
            assertEquals(5, consumer.position(tp));
        }
    }

    @Test
    void groupCommitsAndResumes() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 1, 10);
        TopicPartition tp = new TopicPartition(topic, 0);

        Properties props = consumerProps(group);
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", "4");
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> first = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + 15_000;
            while (first.isEmpty() && System.currentTimeMillis() < deadline) {
                first = consumer.poll(Duration.ofMillis(200));
            }
            assertEquals(4, first.count());
            assertEquals(Set.of(tp), consumer.assignment());
            consumer.commitSync();
            assertEquals(
                    new OffsetAndMetadata(4), consumer.committed(Set.of(tp)).get(tp));
        }
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> rest = drain(consumer, 6);
            assertEquals(4, rest.get(0).offset(), "second member resumes after the committed offset");
            assertEquals(6, rest.size());
        }
    }

    @Test
    void autoCommitOnCloseAndLatestReset() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 1, 3);

        Properties latest = consumerProps(group);
        latest.put("auto.offset.reset", "latest");
        try (var consumer = new IggyKafkaConsumer<String, String>(latest)) {
            consumer.subscribe(List.of(topic));
            assertTrue(drainFor(consumer, 1000).isEmpty(), "latest skips existing records");
            produce(topic, 1, 2);
            assertEquals(
                    List.of(3L, 4L),
                    drain(consumer, 2).stream().map(ConsumerRecord::offset).toList());
        }
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(group))) {
            consumer.subscribe(List.of(topic));
            assertTrue(drainFor(consumer, 1000).isEmpty(), "close committed offset 5");
            assertEquals(
                    5,
                    consumer.committed(Set.of(new TopicPartition(topic, 0)))
                            .get(new TopicPartition(topic, 0))
                            .offset());
        }
    }

    private static List<ConsumerRecord<String, String>> drainFor(
            org.apache.kafka.clients.consumer.Consumer<String, String> consumer, long millis) {
        List<ConsumerRecord<String, String>> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(100)).forEach(all::add);
        }
        return all;
    }

    @Test
    void groupMembersSharePartitions() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 2, 20);

        List<Collection<TopicPartition>> assigned = new CopyOnWriteArrayList<>();
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                assigned.add(partitions);
            }
        };

        try (var a = new IggyKafkaConsumer<String, String>(consumerProps(group));
                var b = new IggyKafkaConsumer<String, String>(consumerProps(group))) {
            a.subscribe(List.of(topic), listener);
            b.subscribe(List.of(topic));
            Set<String> seen = new HashSet<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while ((seen.size() < 20
                            || a.assignment().size() != 1
                            || b.assignment().size() != 1)
                    && System.currentTimeMillis() < deadline) {
                Consumer<ConsumerRecord<String, String>> add = r -> seen.add(r.value());
                a.poll(Duration.ofMillis(100)).forEach(add);
                b.poll(Duration.ofMillis(100)).forEach(add);
            }
            assertEquals(20, seen.size());
            assertEquals(1, a.assignment().size(), "a owns one partition: " + a.assignment());
            assertEquals(1, b.assignment().size(), "b owns one partition: " + b.assignment());
            assertTrue(!assigned.isEmpty(), "rebalance listener was told about the assignment");
        }
    }

    @Test
    void manualAssignmentWithGroupCommits() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 1, 6);
        TopicPartition tp = new TopicPartition(topic, 0);

        Properties props = consumerProps(group);
        props.put("enable.auto.commit", "false");
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.assign(List.of(tp));
            assertEquals(6, drain(consumer, 6).size());
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(3)));
        }
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.assign(List.of(tp));
            assertEquals(3, consumer.position(tp));
            assertEquals(
                    List.of(3L, 4L, 5L),
                    drain(consumer, 3).stream().map(ConsumerRecord::offset).toList());
        }
    }

    @Test
    void patternSubscriptionFindsMatchingTopics() throws Exception {
        String prefix = "p" + UUID.randomUUID().toString().substring(0, 6);
        produce(prefix + "-a", 1, 2);
        produce(prefix + "-b", 1, 3);
        produce("other-" + prefix, 1, 4);
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps("g-" + UUID.randomUUID()))) {
            consumer.subscribe(java.util.regex.Pattern.compile(prefix + "-.*"));
            List<ConsumerRecord<String, String>> records = drain(consumer, 5);
            assertEquals(5, records.size());
            assertEquals(Set.of(prefix + "-a", prefix + "-b"), consumer.subscription());
        }
    }

    @Test
    void deletingASubscribedTopicDoesNotFailPoll() throws Exception {
        String prefix = "d" + UUID.randomUUID().toString().substring(0, 6);
        produce(prefix + "-a", 1, 3);
        produce(prefix + "-b", 1, 3);
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps("g-" + UUID.randomUUID()))) {
            consumer.subscribe(java.util.regex.Pattern.compile(prefix + "-.*"));
            assertEquals(6, drain(consumer, 6).size());

            try (var admin = org.apache.iggy.client.blocking.tcp.IggyTcpClient.builder()
                    .host(iggy.getHost())
                    .port(iggy.getMappedPort(8090))
                    .credentials("iggy", "iggy")
                    .buildAndLogin()) {
                admin.topics()
                        .deleteTopic(
                                org.apache.iggy.identifier.StreamId.of("kafka"),
                                org.apache.iggy.identifier.TopicId.of(prefix + "-b"));
            }
            produce(prefix + "-a", 1, 2);

            // Polls keep working through the deletion, and the topic drops out of the subscription.
            List<ConsumerRecord<String, String>> more = drain(consumer, 2);
            assertEquals(2, more.size());
            assertEquals(Set.of(prefix + "-a"), consumer.subscription());
        }
    }

    @Test
    void unkeyedRecordsAreSpreadByTheServer() throws Exception {
        String topic = topic();
        Properties props = producerProps(3);
        props.put("linger.ms", "0");
        List<RecordMetadata> sent = new ArrayList<>();
        try (Producer<String, String> producer = new IggyKafkaProducer<>(props)) {
            for (int i = 0; i < 9; i++) {
                sent.add(producer.send(new ProducerRecord<>(topic, "v" + i))
                        .get(30, java.util.concurrent.TimeUnit.SECONDS));
            }
        }
        assertEquals(3, sent.stream().map(RecordMetadata::partition).distinct().count(), "all partitions used");

        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(null))) {
            consumer.assign(
                    List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)));
            Map<String, String> where = new java.util.HashMap<>();
            drain(consumer, 9).forEach(r -> where.put(r.value(), r.partition() + "@" + r.offset()));
            for (int i = 0; i < 9; i++) {
                assertEquals(
                        sent.get(i).partition() + "@" + sent.get(i).offset(),
                        where.get("v" + i),
                        "metadata names where the record really is");
            }
        }
    }

    @Test
    void keyedRecordsSeePartitionsAddedLater() throws Exception {
        String topic = topic();
        Properties props = producerProps(1);
        props.put("metadata.max.age.ms", "200");
        try (Producer<String, String> producer = new IggyKafkaProducer<>(props)) {
            assertEquals(
                    0,
                    producer.send(new ProducerRecord<>(topic, "k", "v")).get().partition());

            try (var admin = org.apache.iggy.client.blocking.tcp.IggyTcpClient.builder()
                    .host(iggy.getHost())
                    .port(iggy.getMappedPort(8090))
                    .credentials("iggy", "iggy")
                    .buildAndLogin()) {
                admin.partitions()
                        .createPartitions(
                                org.apache.iggy.identifier.StreamId.of("kafka"),
                                org.apache.iggy.identifier.TopicId.of(topic),
                                2L);
            }
            Thread.sleep(300);

            for (int i = 0; i < 20; i++) {
                String key = "key-" + i;
                int expected = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 3;
                assertEquals(
                        expected,
                        producer.send(new ProducerRecord<>(topic, key, "v"))
                                .get()
                                .partition());
            }
        }
    }

    @Test
    void fullBufferBlocksThenTimesOut() throws Exception {
        String topic = topic();
        Properties props = producerProps(1);
        props.put("buffer.memory", "4096");
        props.put("max.block.ms", "300");
        try (Producer<String, String> producer = new IggyKafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "warm-up")).get(); // topic exists before the pause

            assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> producer.send(new ProducerRecord<>(topic, "x".repeat(5000)))
                            .get(),
                    "a record bigger than the whole buffer fails at once");

            var docker = iggy.getDockerClient();
            docker.pauseContainerCmd(iggy.getContainerId()).exec();
            try {
                List<java.util.concurrent.Future<RecordMetadata>> sent = new ArrayList<>();
                long start = System.currentTimeMillis();
                for (int i = 0; i < 40; i++) {
                    sent.add(producer.send(new ProducerRecord<>(topic, "v".repeat(100))));
                }
                long blocked = System.currentTimeMillis() - start;
                assertTrue(blocked >= 300, "send blocked once the buffer was full: " + blocked + " ms");
                var last = assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> sent.get(sent.size() - 1).get(5, java.util.concurrent.TimeUnit.SECONDS));
                assertTrue(last.getCause() instanceof org.apache.kafka.common.errors.TimeoutException, last.toString());
            } finally {
                docker.unpauseContainerCmd(iggy.getContainerId()).exec();
            }
        }
    }

    @Test
    void runsOnTheExpectedKafkaClientsVersion() throws Exception {
        String expected = System.getProperty("expected.kafka.version", "4.3.0");
        assertEquals(expected, org.apache.kafka.common.utils.AppInfoParser.getVersion());

        // Methods a 3.x caller can still use, removed from the 4.x interfaces.
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        TopicPartition tp = new TopicPartition(topic, 0);
        produce(topic, 1, 3);
        Properties props = consumerProps(group);
        props.put("enable.auto.commit", "false");
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            int read = 0;
            long deadline = System.currentTimeMillis() + 15_000;
            while (read < 3 && System.currentTimeMillis() < deadline) {
                read += consumer.poll(200L).count();
            }
            assertEquals(3, read);
            consumer.commitSync();
            assertEquals(3, consumer.committed(tp).offset());
            assertEquals(3, consumer.committed(tp, Duration.ofSeconds(5)).offset());
        }
    }

    private static double metric(
            Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics,
            String group,
            String name) {
        return metrics.entrySet().stream()
                .filter(e ->
                        e.getKey().group().equals(group) && e.getKey().name().equals(name))
                .map(e -> ((Number) e.getValue().metricValue()).doubleValue())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no metric " + group + "/" + name));
    }

    @Test
    void metricsCountWhatHappened() throws Exception {
        String topic = topic();
        Properties producerProps = producerProps(1);
        producerProps.put("client.id", "metrics-test");
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps)) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "v" + i));
            }
            producer.flush();
            var m = producer.metrics();
            assertEquals(5, metric(m, "producer-metrics", "record-send-total"));
            assertEquals(0, metric(m, "producer-metrics", "record-error-total"));
            assertEquals(32 * 1024 * 1024, metric(m, "producer-metrics", "buffer-available-bytes"));
            assertTrue(metric(m, "producer-metrics", "request-latency-avg") > 0);
            assertTrue(m.keySet().stream()
                    .allMatch(n -> !n.group().equals("producer-metrics")
                            || "metrics-test".equals(n.tags().get("client-id"))));
        }

        String group = "g-" + UUID.randomUUID();
        Properties props = consumerProps(group);
        props.put("enable.auto.commit", "false");
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            assertEquals(5, drain(consumer, 5).size());
            consumer.commitSync();
            var m = consumer.metrics();
            assertEquals(5, metric(m, "consumer-fetch-manager-metrics", "records-consumed-total"));
            assertEquals(1, metric(m, "consumer-coordinator-metrics", "commit-total"));
            assertEquals(1, metric(m, "consumer-coordinator-metrics", "assigned-partitions"));
        }
    }

    @Test
    void bufferedRecordsAreReturnedInOrderAndOnlyReturnedOnesAreCommitted() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 1, 10);
        TopicPartition tp = new TopicPartition(topic, 0);

        Properties props = consumerProps(group);
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", "2");
        try (var consumer = new IggyKafkaConsumer<String, String>(props)) {
            consumer.assign(List.of(tp));
            List<Long> first =
                    drain(consumer, 2).stream().map(ConsumerRecord::offset).toList();
            List<Long> second =
                    drain(consumer, 2).stream().map(ConsumerRecord::offset).toList();
            assertEquals(List.of(0L, 1L), first);
            assertEquals(List.of(2L, 3L), second, "the next poll continues from the buffer");
            assertEquals(4, consumer.position(tp));

            consumer.commitSync();
            assertEquals(
                    4,
                    consumer.committed(Set.of(tp)).get(tp).offset(),
                    "records fetched but not returned are not committed");

            consumer.seek(tp, 1);
            assertEquals(
                    List.of(1L, 2L),
                    drain(consumer, 2).stream().map(ConsumerRecord::offset).toList(),
                    "a seek discards the buffer");
        }
    }

    @Test
    void kafkaErrorsForMisuse() {
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(null))) {
            assertThrows(InvalidGroupIdException.class, () -> consumer.subscribe(List.of("x")));
            consumer.assign(List.of(new TopicPartition(topic(), 0)));
            consumer.wakeup();
            assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofMillis(10)));
        }
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(1))) {
            assertThrows(UnsupportedOperationException.class, producer::initTransactions);
        }
    }

    @Test
    void sendToAMissingPartitionFailsTheRecord() throws Exception {
        String topic = topic();
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(1))) {
            AtomicReference<Exception> callbackError = new AtomicReference<>();
            Future<RecordMetadata> future =
                    producer.send(new ProducerRecord<>(topic, 5, "k", "v"), (metadata, e) -> callbackError.set(e));
            ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, e.getCause());
            assertInstanceOf(TimeoutException.class, callbackError.get());
        }
    }

    @Test
    @Timeout(30)
    void flushFromACallbackThrowsInsteadOfHanging() throws Exception {
        String topic = topic();
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(1))) {
            AtomicReference<Exception> flushError = new AtomicReference<>();
            producer.send(new ProducerRecord<>(topic, "k", "v"), (metadata, e) -> {
                try {
                    producer.flush();
                } catch (Exception thrown) {
                    flushError.set(thrown);
                }
            });
            producer.flush();
            assertInstanceOf(KafkaException.class, flushError.get());
            producer.send(new ProducerRecord<>(topic, "k", "v2")).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    @Timeout(90)
    void movedPartitionIsHandedOverOnceItsBufferIsDrained() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        produce(topic, 2, 600);
        Properties props = consumerProps(group);
        props.put("max.poll.records", "10");

        Set<String> seen = new HashSet<>();
        try (var a = new IggyKafkaConsumer<String, String>(props)) {
            a.subscribe(List.of(topic));
            Set<Integer> started = new HashSet<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (started.size() < 2 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : a.poll(Duration.ofMillis(100))) {
                    seen.add(r.value());
                    started.add(r.partition());
                }
            }
            assertEquals(2, started.size(), "a has records buffered from both partitions");

            try (var b = new IggyKafkaConsumer<String, String>(props)) {
                b.subscribe(List.of(topic));
                long joined = System.currentTimeMillis();
                while (b.assignment().isEmpty() && System.currentTimeMillis() - joined < 25_000) {
                    a.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                    b.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                }
                long handOff = System.currentTimeMillis() - joined;
                assertEquals(1, b.assignment().size(), "b owns a partition: " + b.assignment());
                assertTrue(handOff < 15_000, "hand-off took " + handOff + " ms; Iggy's rebalancing timeout is 30 s");

                deadline = System.currentTimeMillis() + 20_000;
                while (seen.size() < 600 && System.currentTimeMillis() < deadline) {
                    a.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                    b.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                }
                assertEquals(600, seen.size(), "every record was read");
            }
        }
    }
}
