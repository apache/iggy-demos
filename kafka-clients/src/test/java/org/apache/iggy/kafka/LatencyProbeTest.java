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
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * End-to-end latency, library vs plain SDK, at the same fixed send rate on the same server.
 * Latency is from each message's scheduled send time to when the consumer has it. Excluded
 * from the default build; run with {@code mvn test -Pbenchmark -Dtest=LatencyProbeTest}.
 */
@Tag("benchmark")
class LatencyProbeTest {
    static final int PARTITIONS = 4;
    static final int SIZE = 100;
    static final int SECONDS = 10;

    String host;
    int port;
    StreamId stream = StreamId.of("kafka");
    int topicNumber;

    static byte[] payload(long scheduled) {
        return ByteBuffer.allocate(SIZE).putLong(scheduled).array();
    }

    static long stamp(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }

    static Message message(long scheduled) {
        return new Message(
                new MessageHeader(
                        BigInteger.ZERO,
                        MessageId.serverGenerated(),
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        0L,
                        (long) SIZE,
                        BigInteger.ZERO),
                payload(scheduled),
                Map.of());
    }

    IggyTcpClient client() {
        return IggyTcpClient.builder()
                .host(host)
                .port(port)
                .credentials("iggy", "iggy")
                .buildAndLogin();
    }

    String newTopic(IggyTcpClient c) {
        String topic = "lat" + topicNumber++;
        c.topics()
                .createTopic(
                        stream, (long) PARTITIONS, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, topic);
        return topic;
    }

    /** Library producer and consumer, default settings. */
    long[] library(String topic, int rate) throws Exception {
        int count = rate * SECONDS;
        long[] latencies = new long[count];
        Properties base = new Properties();
        base.put("bootstrap.servers", host + ":" + port);
        base.put("iggy.password", "iggy");
        Properties cp = new Properties();
        cp.putAll(base);
        cp.put("key.deserializer", ByteArrayDeserializer.class.getName());
        cp.put("value.deserializer", ByteArrayDeserializer.class.getName());
        Properties pp = new Properties();
        pp.putAll(base);
        pp.put("key.serializer", ByteArraySerializer.class.getName());
        pp.put("value.serializer", ByteArraySerializer.class.getName());
        int[] received = {0};
        AtomicBoolean stop = new AtomicBoolean();
        try (var consumer = new IggyKafkaConsumer<byte[], byte[]>(cp);
                var producer = new IggyKafkaProducer<byte[], byte[]>(pp)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < PARTITIONS; p++) {
                tps.add(new TopicPartition(topic, p));
            }
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            producer.partitionsFor(topic);
            Thread reader = new Thread(() -> {
                while (!stop.get() && received[0] < count) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(100))) {
                        long now = System.nanoTime();
                        if (received[0] < count) {
                            latencies[received[0]++] = now - stamp(r.value());
                        }
                    }
                }
            });
            reader.start();
            Thread.sleep(500);
            long gap = 1_000_000_000L / rate;
            long next = System.nanoTime();
            for (int i = 0; i < count; i++) {
                while (System.nanoTime() < next) {
                    Thread.onSpinWait();
                }
                producer.send(new ProducerRecord<>(topic, i % PARTITIONS, null, payload(next)));
                next += gap;
            }
            producer.flush();
            reader.join(15_000);
            stop.set(true);
            reader.join();
        }
        return Arrays.copyOf(latencies, received[0]);
    }

    /**
     * Plain SDK. The producer sends whatever is due, grouped by partition, once the oldest unsent
     * message has waited {@code lingerMs}. The consumer polls each partition in turn and sleeps
     * {@code idleMs} when a full pass finds nothing.
     */
    long[] sdk(String topic, int rate, long lingerMs, long idleMs) throws Exception {
        int count = rate * SECONDS;
        long[] latencies = new long[count];
        int[] received = {0};
        AtomicBoolean stop = new AtomicBoolean();
        try (var pc = client();
                var cc = client()) {
            TopicId t = TopicId.of(topic);
            Thread reader = new Thread(() -> {
                long[] positions = new long[PARTITIONS];
                Consumer consumer = Consumer.of(1L);
                while (!stop.get() && received[0] < count) {
                    boolean any = false;
                    for (int p = 0; p < PARTITIONS; p++) {
                        PolledMessages polled = cc.messages()
                                .pollMessages(
                                        stream,
                                        t,
                                        Optional.of((long) p),
                                        consumer,
                                        PollingStrategy.offset(BigInteger.valueOf(positions[p])),
                                        1000L,
                                        false);
                        long now = System.nanoTime();
                        for (Message m : polled.messages()) {
                            long offset = m.header().offset().longValue();
                            if (offset < positions[p]) {
                                continue;
                            }
                            positions[p] = offset + 1;
                            any = true;
                            if (received[0] < count) {
                                latencies[received[0]++] = now - stamp(m.payload());
                            }
                        }
                    }
                    if (!any) {
                        try {
                            Thread.sleep(idleMs);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            });
            reader.start();
            Thread.sleep(500);
            long gap = 1_000_000_000L / rate;
            long linger = lingerMs * 1_000_000L;
            long start = System.nanoTime();
            int scheduled = 0;
            int sent = 0;
            while (sent < count) {
                long now = System.nanoTime();
                while (scheduled < count && start + scheduled * gap <= now) {
                    scheduled++;
                }
                boolean due = scheduled > sent && (scheduled == count || now - (start + sent * gap) >= linger);
                if (!due) {
                    Thread.onSpinWait();
                    continue;
                }
                List<List<Message>> batches = new ArrayList<>();
                for (int p = 0; p < PARTITIONS; p++) {
                    batches.add(new ArrayList<>());
                }
                for (int i = sent; i < scheduled; i++) {
                    batches.get(i % PARTITIONS).add(message(start + i * gap));
                }
                for (int p = 0; p < PARTITIONS; p++) {
                    if (!batches.get(p).isEmpty()) {
                        pc.messages().sendMessages(stream, t, Partitioning.partitionId((long) p), batches.get(p));
                    }
                }
                sent = scheduled;
            }
            reader.join(15_000);
            stop.set(true);
            reader.join();
        }
        return Arrays.copyOf(latencies, received[0]);
    }

    static String summary(String name, int rate, long[] ns) {
        long[] s = ns.clone();
        Arrays.sort(s);
        double p50 = s[s.length / 2] / 1e6;
        double p99 = s[(int) (s.length * 0.99)] / 1e6;
        double p999 = s[(int) (s.length * 0.999)] / 1e6;
        double max = s[s.length - 1] / 1e6;
        return String.format(
                "LAT %,6d/s %-32s p50 %7.2f  p99 %7.2f  p99.9 %7.2f  max %8.2f ms  (n=%,d of %,d)",
                rate, name, p50, p99, p999, max, s.length, rate * SECONDS);
    }

    @Test
    void probe() throws Exception {
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
            host = iggy.getHost();
            port = iggy.getMappedPort(8090);
            try (var c = client()) {
                c.streams().createStream("kafka");
                for (int rate : new int[] {200_000, 500_000, 1_000_000}) {
                    for (int round = 0; round < 4; round++) {
                        String tag = round == 0 ? " (warm-up)" : "";
                        System.out.println(summary("library (defaults)" + tag, rate, library(newTopic(c), rate)));
                        System.out.println(
                                summary("SDK, 5 ms linger, 20 ms idle" + tag, rate, sdk(newTopic(c), rate, 5, 20)));
                        System.out.println(
                                summary("SDK, no linger, 1 ms idle" + tag, rate, sdk(newTopic(c), rate, 0, 1)));
                    }
                }
            }
        }
    }
}
