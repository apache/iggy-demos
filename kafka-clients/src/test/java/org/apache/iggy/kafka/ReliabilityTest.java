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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Failure, concurrency and group membership cases, on a server of their own. */
class ReliabilityTest {

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
        return "r-" + UUID.randomUUID().toString().substring(0, 8);
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
        p.put("auto.commit.interval.ms", "200");
        if (group != null) {
            p.put("group.id", group);
        }
        return p;
    }

    private static List<ConsumerRecord<String, String>> readAll(String topic, int partitions, int expected) {
        try (var consumer = new IggyKafkaConsumer<String, String>(consumerProps(null))) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                tps.add(new TopicPartition(topic, p));
            }
            consumer.assign(tps);
            List<ConsumerRecord<String, String>> all = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (all.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(all::add);
            }
            return all;
        }
    }

    @Test
    void sendFailsWhileServerIsDownAndWorksAfterwards() throws Exception {
        String topic = topic();
        Properties props = producerProps(1);
        props.put("request.timeout.ms", "1000");
        try (Producer<String, String> producer = new IggyKafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "before")).get();

            List<Exception> callbackErrors = new CopyOnWriteArrayList<>();
            iggy.getDockerClient().pauseContainerCmd(iggy.getContainerId()).exec();
            Future<RecordMetadata> during;
            try {
                during = producer.send(new ProducerRecord<>(topic, "during"), (m, e) -> {
                    if (e != null) {
                        callbackErrors.add(e);
                    }
                });
                ExecutionException failed = org.junit.jupiter.api.Assertions.assertThrows(
                        ExecutionException.class, () -> during.get(30, TimeUnit.SECONDS));
                assertTrue(failed.getCause() instanceof org.apache.kafka.common.KafkaException, failed.toString());
                assertEquals(1, callbackErrors.size(), "the callback saw the failure");
            } finally {
                iggy.getDockerClient()
                        .unpauseContainerCmd(iggy.getContainerId())
                        .exec();
            }

            // The SDK reconnects in the background; once it has, sends work again.
            RecordMetadata after = null;
            long deadline = System.currentTimeMillis() + 60_000;
            while (after == null && System.currentTimeMillis() < deadline) {
                try {
                    after = producer.send(new ProducerRecord<>(topic, "after")).get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    Thread.sleep(500);
                }
            }
            assertTrue(after != null, "sends work again after the server comes back");
        }
    }

    @Test
    void manyThreadsSendingKeepPerKeyOrder() throws Exception {
        String topic = topic();
        int threads = 8;
        int perThread = 500;
        Properties props = producerProps(4);
        try (Producer<String, String> producer = new IggyKafkaProducer<>(props)) {
            producer.partitionsFor(topic); // create the topic before the threads race
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<List<Future<RecordMetadata>>>> work = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                String key = "thread-" + t;
                work.add(pool.submit(() -> {
                    List<Future<RecordMetadata>> sent = new ArrayList<>();
                    for (int i = 0; i < perThread; i++) {
                        sent.add(producer.send(new ProducerRecord<>(topic, key, String.valueOf(i))));
                    }
                    return sent;
                }));
            }
            for (var w : work) {
                for (var f : w.get()) {
                    f.get(30, TimeUnit.SECONDS);
                }
            }
            pool.shutdown();
        }

        List<ConsumerRecord<String, String>> records = readAll(topic, 4, threads * perThread);
        assertEquals(threads * perThread, records.size());
        Map<String, Integer> last = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            int n = Integer.parseInt(r.value());
            int previous = last.getOrDefault(r.key(), -1);
            assertEquals(previous + 1, n, "records for " + r.key() + " arrive in send order");
            last.put(r.key(), n);
        }
    }

    @Test
    void closeSendsRecordsStillInFlight() throws Exception {
        String topic = topic();
        int count = 2000;
        List<Future<RecordMetadata>> sent = new ArrayList<>();
        Properties props = producerProps(1);
        props.put("linger.ms", "50");
        Producer<String, String> producer = new IggyKafkaProducer<>(props);
        for (int i = 0; i < count; i++) {
            sent.add(producer.send(new ProducerRecord<>(topic, "v" + i)));
        }
        producer.close(); // no flush first
        for (Future<RecordMetadata> f : sent) {
            assertTrue(f.isDone(), "close waits for every record");
            f.get();
        }
        assertEquals(count, readAll(topic, 1, count).size());
    }

    @Test
    void membersJoiningOneByOneEndWithOnePartitionEach() throws Exception {
        String topic = topic();
        String group = "g-" + UUID.randomUUID();
        try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps(3))) {
            for (int i = 0; i < 300; i++) {
                producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i));
            }
            producer.flush();
        }

        List<Collection<TopicPartition>> revokedFromFirst = new CopyOnWriteArrayList<>();
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                revokedFromFirst.add(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
        };

        Set<String> seen = new HashSet<>();
        List<IggyKafkaConsumer<String, String>> members = new ArrayList<>();
        try {
            for (int m = 0; m < 3; m++) {
                var member = new IggyKafkaConsumer<String, String>(consumerProps(group));
                if (m == 0) {
                    member.subscribe(List.of(topic), listener);
                } else {
                    member.subscribe(List.of(topic));
                }
                members.add(member);
                // Let the group settle with this many members before the next one joins.
                long settle = System.currentTimeMillis() + 3_000;
                while (System.currentTimeMillis() < settle) {
                    for (var c : members) {
                        c.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                    }
                }
            }
            long deadline = System.currentTimeMillis() + 30_000;
            while (!members.stream().allMatch(c -> c.assignment().size() == 1)
                    && System.currentTimeMillis() < deadline) {
                for (var c : members) {
                    c.poll(Duration.ofMillis(50)).forEach(r -> seen.add(r.value()));
                }
            }
            Set<TopicPartition> union = new HashSet<>();
            for (var c : members) {
                assertEquals(1, c.assignment().size(), "each member owns one partition: " + c.assignment());
                union.addAll(c.assignment());
            }
            assertEquals(3, union.size(), "no partition is owned twice");
            assertEquals(300, seen.size(), "every record was read at least once");
            assertTrue(!revokedFromFirst.isEmpty(), "the first member was told it lost partitions");
        } finally {
            members.forEach(IggyKafkaConsumer::close);
        }
    }
}
