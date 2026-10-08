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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The same benchmark code against Iggy (through this library) and Kafka (through the stock Kafka
 * clients). Only the constructed classes differ. Each broker runs on its own. Excluded from the
 * default build; run with {@code mvn test -Pbenchmark}.
 */
@Tag("benchmark")
class PerfBenchmarkTest {

    static final int PARTITIONS = 4;
    static final int RECORDS = 1_000_000;

    record Target(
            String name,
            Properties base,
            Consumer<String> createTopic,
            Function<Properties, Producer<?, ?>> producer,
            Function<Properties, org.apache.kafka.clients.consumer.Consumer<?, ?>> consumer) {

        Properties props() {
            Properties p = new Properties();
            p.putAll(base);
            return p;
        }
    }

    @SuppressWarnings("unchecked")
    static double produce(Target t, String topic, int size) {
        Properties p = t.props();
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        byte[] value = new byte[size];
        java.util.concurrent.atomic.AtomicLong failed = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicReference<Exception> firstError =
                new java.util.concurrent.atomic.AtomicReference<>();
        long start;
        long end;
        // Timed from the first send to flush(); creating and closing the client are not part of it.
        try (var producer = (Producer<byte[], byte[]>) t.producer().apply(p)) {
            start = System.nanoTime();
            for (int i = 0; i < RECORDS; i++) {
                producer.send(new ProducerRecord<>(topic, i % PARTITIONS, null, value), (m, e) -> {
                    if (e != null) {
                        failed.incrementAndGet();
                        firstError.compareAndSet(null, e);
                    }
                });
            }
            producer.flush();
            end = System.nanoTime();
        }
        double rate = RECORDS / ((end - start) / 1e9);
        if (failed.get() > 0) {
            throw new IllegalStateException(t.name() + ": " + failed.get() + " sends failed", firstError.get());
        }
        return rate;
    }

    @SuppressWarnings("unchecked")
    static double consume(Target t, String topic) {
        Properties p = t.props();
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("auto.offset.reset", "earliest");
        long start;
        long end;
        // Timed from the first poll to the last record; creating and closing the client are not part of it.
        try (var consumer = (org.apache.kafka.clients.consumer.Consumer<byte[], byte[]>)
                t.consumer().apply(p)) {
            consumer.assign(partitions(topic));
            start = System.nanoTime();
            int read = 0;
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (read < RECORDS) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(t.name() + ": read only " + read + " of " + RECORDS);
                }
                read += consumer.poll(Duration.ofMillis(100)).count();
            }
            end = System.nanoTime();
        }
        return RECORDS / ((end - start) / 1e9);
    }

    static List<TopicPartition> partitions(String topic) {
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            tps.add(new TopicPartition(topic, i));
        }
        return tps;
    }

    /** Sends at a steady rate; returns {median, p99, count} of send-to-receive time in ms. */
    @SuppressWarnings("unchecked")
    static double[] latency(Target t, String topic, int perSecond, int count) throws Exception {
        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean stop = new AtomicBoolean();
        Properties cp = t.props();
        cp.put("key.deserializer", ByteArrayDeserializer.class.getName());
        cp.put("value.deserializer", StringDeserializer.class.getName());
        Properties pp = t.props();
        pp.put("key.serializer", ByteArraySerializer.class.getName());
        pp.put("value.serializer", StringSerializer.class.getName());
        try (var consumer = (org.apache.kafka.clients.consumer.Consumer<byte[], String>)
                        t.consumer().apply(cp);
                var producer = (Producer<byte[], String>) t.producer().apply(pp)) {
            consumer.assign(partitions(topic));
            consumer.seekToEnd(partitions(topic));
            partitions(topic).forEach(consumer::position);
            Thread reader = new Thread(() -> {
                while (!stop.get()) {
                    consumer.poll(Duration.ofMillis(100))
                            .forEach(r -> latencies.add((System.nanoTime() - Long.parseLong(r.value())) / 1e6));
                }
            });
            reader.start();
            Thread.sleep(500);
            long gap = 1_000_000_000L / perSecond;
            long next = System.nanoTime();
            for (int i = 0; i < count; i++) {
                while (System.nanoTime() < next) {
                    Thread.onSpinWait();
                }
                producer.send(new ProducerRecord<>(topic, i % PARTITIONS, null, String.valueOf(System.nanoTime())));
                next += gap;
            }
            producer.flush();
            Thread.sleep(1000);
            stop.set(true);
            reader.join();
        }
        List<Double> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        return new double[] {sorted.get(sorted.size() / 2), sorted.get((int) (sorted.size() * 0.99)), sorted.size()};
    }

    static void run(Target t) throws Exception {
        int run = 0;
        for (int round = 0; round < 4; round++) {
            String tag = round == 0 ? " (warm-up)" : "";
            for (int size : new int[] {100, 1024}) {
                String topic = "t" + run++;
                t.createTopic().accept(topic);
                double p = produce(t, topic, size);
                double c = consume(t, topic);
                System.out.printf(
                        "PERF%s %-16s %4d B | produce %,8.0f rec/s (%5.1f MB/s) | consume %,9.0f rec/s%n",
                        tag, t.name(), size, p, p * size / 1e6, c);
            }
        }
        for (int round = 0; round < 3; round++) {
            String topic = "lat" + run++;
            t.createTopic().accept(topic);
            double[] l = latency(t, topic, 1000, 5000);
            System.out.printf(
                    "PERF latency %-16s at 1,000 rec/s: median %.1f ms, p99 %.1f ms (n=%.0f)%n",
                    t.name(), l[0], l[1], l[2]);
        }
    }

    @Test
    void iggy() throws Exception {
        try (var iggy = new GenericContainer<>("apache/iggy:0.9.0")
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
                        .withStartupTimeout(Duration.ofSeconds(60)))) {
            iggy.start();
            Properties base = new Properties();
            base.put("bootstrap.servers", iggy.getHost() + ":" + iggy.getMappedPort(8090));
            base.put("iggy.password", "iggy");
            base.put("iggy.default.partitions", String.valueOf(PARTITIONS));
            run(new Target(
                    "Iggy via library",
                    base,
                    topic -> {
                        Properties p = new Properties();
                        p.putAll(base);
                        p.put("key.serializer", ByteArraySerializer.class.getName());
                        p.put("value.serializer", ByteArraySerializer.class.getName());
                        try (var producer = new IggyKafkaProducer<byte[], byte[]>(p)) {
                            producer.partitionsFor(topic);
                        }
                    },
                    IggyKafkaProducer::new,
                    IggyKafkaConsumer::new));
        }
    }

    @Test
    void kafka() throws Exception {
        try (var kafka = new KafkaContainer("apache/kafka:4.3.1")) {
            kafka.start();
            Properties base = new Properties();
            base.put("bootstrap.servers", kafka.getBootstrapServers());
            // The library has no idempotence, so compare with it off; acks stays at the default, all.
            // With it on, Kafka 4.3.1 in Testcontainers failed 1 KB runs with OutOfOrderSequenceException.
            base.put("enable.idempotence", "false");
            run(new Target(
                    "Kafka",
                    base,
                    topic -> {
                        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                            admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1)))
                                    .all()
                                    .get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    KafkaProducer::new,
                    KafkaConsumer::new));
        }
    }
}
