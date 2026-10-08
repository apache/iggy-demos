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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the Kafka-shaped admin client against a real Iggy server in Docker. */
@SuppressWarnings("deprecation") // ConsumerGroupState is the group state type kafka-clients 3.x has
class IggyAdminClientTest {

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

    private static String name(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("iggy.password", "iggy");
        return p;
    }

    private static Properties producerProps() {
        Properties p = adminProps();
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        return p;
    }

    private static Properties consumerProps(String group, int maxPollRecords) {
        Properties p = adminProps();
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("max.poll.records", String.valueOf(maxPollRecords));
        p.put("group.id", group);
        return p;
    }

    private static <T> T get(KafkaFuture<T> future) throws Exception {
        return future.get(30, TimeUnit.SECONDS);
    }

    private static void assertFails(Class<? extends Throwable> expected, KafkaFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> get(future));
        assertInstanceOf(expected, e.getCause(), () -> "cause was " + e.getCause());
    }

    private static List<ConsumerRecord<String, String>> pollUntil(Consumer<String, String> consumer, int count) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (records.size() < count && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
        }
        assertEquals(count, records.size(), "records received");
        return records;
    }

    @Test
    void describesTheCluster() throws Exception {
        try (Admin admin = new IggyAdminClient(adminProps())) {
            DescribeClusterResult cluster = admin.describeCluster();
            assertFalse(get(cluster.nodes()).isEmpty());
            assertNotNull(get(cluster.controller()));
            assertNotNull(get(cluster.clusterId()));
            assertNull(get(cluster.authorizedOperations()));
        }
    }

    @Test
    void managesTopics() throws Exception {
        String topic = name("admin-topic");
        try (Admin admin = IggyAdminClient.create(adminProps())) {
            NewTopic request = new NewTopic(topic, 3, (short) 1).configs(Map.of("retention.ms", "86400000"));
            assertEquals(
                    3,
                    get(admin.createTopics(List.of(request)).numPartitions(topic))
                            .intValue());
        }
        try (Admin admin = new IggyAdminClient(adminProps())) {
            assertFails(
                    TopicExistsException.class,
                    admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                            .all());

            Map<String, TopicListing> listings = get(admin.listTopics().namesToListings());
            assertTrue(listings.containsKey(topic));

            TopicDescription description =
                    get(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
            assertEquals(3, description.partitions().size());
            assertEquals(
                    List.of(0, 1, 2),
                    description.partitions().stream().map(p -> p.partition()).toList());
            assertNotNull(description.partitions().get(0).leader());

            TopicDescription byId = get(admin.describeTopics(TopicCollection.ofTopicIds(
                                    List.of(listings.get(topic).topicId())))
                            .allTopicIds())
                    .get(listings.get(topic).topicId());
            assertEquals(topic, byId.name());

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var config = get(admin.describeConfigs(List.of(resource)).all()).get(resource);
            assertEquals("86400000", config.get("retention.ms").value());
            assertEquals("-1", config.get("retention.bytes").value());
            assertEquals("none", config.get("compression.type").value());

            get(admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(5)))
                    .all());
            assertEquals(
                    5,
                    get(admin.describeTopics(List.of(topic)).allTopicNames())
                            .get(topic)
                            .partitions()
                            .size());
            assertFails(
                    InvalidPartitionsException.class,
                    admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2)))
                            .all());

            get(admin.deleteTopics(List.of(topic)).all());
            assertFails(
                    UnknownTopicOrPartitionException.class,
                    admin.describeTopics(List.of(topic)).allTopicNames());
            assertFails(
                    UnknownTopicOrPartitionException.class,
                    admin.deleteTopics(List.of(topic)).all());
            assertFails(
                    UnknownTopicOrPartitionException.class,
                    admin.describeConfigs(List.of(resource)).all());
        }
    }

    @Test
    void managesConsumerGroupsAndOffsets() throws Exception {
        String topic = name("admin-group-topic");
        String group = name("admin-group");
        TopicPartition tp = new TopicPartition(topic, 0);
        try (Admin admin = new IggyAdminClient(adminProps())) {
            get(admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all());
            try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps())) {
                for (int i = 0; i < 10; i++) {
                    producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i));
                }
                producer.flush();
            }

            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> offsets =
                    get(admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all());
            assertEquals(0, offsets.get(tp).offset());
            assertEquals(
                    10,
                    get(admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all())
                            .get(tp)
                            .offset());

            try (Consumer<String, String> consumer = new IggyKafkaConsumer<>(consumerProps(group, 4))) {
                consumer.subscribe(List.of(topic));
                pollUntil(consumer, 4);
                consumer.commitSync();

                List<ConsumerGroupListing> groups =
                        new ArrayList<>(get(admin.listConsumerGroups().all()));
                ConsumerGroupListing listing = groups.stream()
                        .filter(g -> g.groupId().equals(group))
                        .findFirst()
                        .orElseThrow();
                assertEquals(Optional.of(ConsumerGroupState.STABLE), listing.state());

                ConsumerGroupDescription description =
                        get(admin.describeConsumerGroups(List.of(group)).all()).get(group);
                assertEquals(ConsumerGroupState.STABLE, description.state());
                assertEquals(1, description.members().size());
                MemberDescription member = description.members().iterator().next();
                assertEquals(Set.of(tp), member.assignment().topicPartitions());

                Map<TopicPartition, OffsetAndMetadata> committed =
                        get(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata());
                assertEquals(4, committed.get(tp).offset());

                assertFails(
                        GroupNotEmptyException.class,
                        admin.deleteConsumerGroups(List.of(group)).all());
                assertFails(
                        GroupNotEmptyException.class,
                        admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(2)))
                                .all());
            }

            assertEquals(
                    ConsumerGroupState.EMPTY,
                    get(admin.describeConsumerGroups(List.of(group)).all())
                            .get(group)
                            .state());

            get(admin.alterConsumerGroupOffsets(group, Map.of(tp, new OffsetAndMetadata(2)))
                    .all());
            assertEquals(
                    2,
                    get(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata())
                            .get(tp)
                            .offset());
            assertEquals(
                    ConsumerGroupState.EMPTY,
                    get(admin.describeConsumerGroups(List.of(group)).all())
                            .get(group)
                            .state());

            try (Consumer<String, String> consumer = new IggyKafkaConsumer<>(consumerProps(group, 1))) {
                consumer.subscribe(List.of(topic));
                assertEquals(2, pollUntil(consumer, 1).get(0).offset());
            }

            get(admin.deleteConsumerGroups(List.of(group)).all());
            assertTrue(get(admin.listConsumerGroups().all()).stream()
                    .noneMatch(g -> g.groupId().equals(group)));
            assertFails(
                    GroupIdNotFoundException.class,
                    admin.deleteConsumerGroups(List.of(group)).all());
            assertFails(
                    GroupIdNotFoundException.class,
                    admin.describeConsumerGroups(List.of(group)).all());

            get(admin.deleteTopics(List.of(topic)).all());
        }
    }

    @Test
    void rejectsWhatIggyCannotDo() {
        try (Admin admin = new IggyAdminClient(adminProps())) {
            assertThrows(UnsupportedOperationException.class, () -> admin.describeAcls(AclBindingFilter.ANY));
            assertThrows(UnsupportedOperationException.class, () -> admin.deleteRecords(Map.of()));
            assertThrows(UnsupportedOperationException.class, () -> admin.listTransactions());
        }
    }

    @Test
    void countsRequestsInKafkaStyleMetrics() throws Exception {
        try (Admin admin = new IggyAdminClient(adminProps())) {
            get(admin.describeCluster().clusterId());
            assertFails(
                    UnknownTopicOrPartitionException.class,
                    admin.describeTopics(List.of(name("missing"))).allTopicNames());
            Map<String, Double> values = new java.util.HashMap<>();
            admin.metrics().forEach((n, m) -> {
                if (n.group().equals("admin-client-metrics")) {
                    values.put(n.name(), ((Number) m.metricValue()).doubleValue());
                }
            });
            assertEquals(3.0, values.get("request-total"), "describeCluster makes two requests, describeTopics one");
            assertEquals(1.0, values.get("failed-request-total"));
            assertTrue(values.containsKey("request-latency-avg"));
        }
    }

    @Test
    void validateOnlyCreatesNothing() throws Exception {
        Properties props = adminProps();
        props.put("iggy.stream", name("s"));
        try (Admin admin = new IggyAdminClient(props)) {
            get(admin.createTopics(
                            List.of(new NewTopic(name("t"), 1, (short) 1)),
                            new CreateTopicsOptions().validateOnly(true))
                    .all());
        }
        try (IggyConnection connection = new IggyConnection(new IggyKafkaConfig(IggyKafkaConfig.toMap(props)))) {
            assertThrows(UnknownTopicOrPartitionException.class, () -> connection.ensureStream(false));
        }
    }

    @Test
    void listingAllOffsetsSkipsUncommittedPartitions() throws Exception {
        String topic = name("t");
        String group = name("g");
        TopicPartition committed = new TopicPartition(topic, 0);
        try (Admin admin = new IggyAdminClient(adminProps())) {
            get(admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all());
            try (Producer<String, String> producer = new IggyKafkaProducer<>(producerProps())) {
                producer.send(new ProducerRecord<>(topic, 0, "k", "v1")).get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topic, 0, "k", "v2")).get(10, TimeUnit.SECONDS);
            }
            get(admin.alterConsumerGroupOffsets(group, Map.of(committed, new OffsetAndMetadata(1)))
                    .all());
            assertEquals(
                    Map.of(committed, new OffsetAndMetadata(1)),
                    get(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()));
        }
    }
}
