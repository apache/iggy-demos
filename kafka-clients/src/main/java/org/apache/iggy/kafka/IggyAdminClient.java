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

import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.cluster.ClusterNode;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.consumergroup.ConsumerGroupMember;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AddRaftVoterOptions;
import org.apache.kafka.clients.admin.AddRaftVoterResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupsResult;
import org.apache.kafka.clients.admin.DeleteStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClassicGroupsOptions;
import org.apache.kafka.clients.admin.DescribeClassicGroupsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
import org.apache.kafka.clients.admin.IggyAdminResults;
import org.apache.kafka.clients.admin.IggyAdminResults4;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RemoveRaftVoterOptions;
import org.apache.kafka.clients.admin.RemoveRaftVoterResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.TerminateTransactionOptions;
import org.apache.kafka.clients.admin.TerminateTransactionResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicIdCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * Kafka's {@link AdminClient}, backed by Apache Iggy.
 *
 * <p>Topics, partitions, consumer groups, committed offsets and log offsets map onto Iggy. Every
 * Kafka topic is an Iggy topic of the same name in the configured stream, as for the producer and
 * consumer. Brokers, ACLs, quotas, credentials, tokens, log directories, reassignments, the Raft
 * quorum, transactions, share groups and streams groups have no Iggy equivalent; those methods throw
 * {@link UnsupportedOperationException}.
 *
 * <p>Calls run synchronously on the caller's thread against the Iggy connection and the returned
 * futures are already complete. The {@code timeoutMs} in each options object is not used;
 * {@code request.timeout.ms} bounds each Iggy request instead.
 */
public class IggyAdminClient extends AdminClient {

    /** Kafka's "never expire" and "unlimited" are Iggy's u64::MAX. */
    private static final BigInteger U64_MAX = BigInteger.TWO.pow(64).subtract(BigInteger.ONE);

    private static final String METRICS_GROUP = "admin-client-metrics";

    private final IggyKafkaConfig config;
    private final IggyConnection connection;
    private final ClientMetrics metrics;
    private final Sensor requestSensor;
    private final Sensor failedRequestSensor;
    private final Sensor latencySensor;

    public IggyAdminClient(Properties properties) {
        this(IggyKafkaConfig.toMap(properties));
    }

    public IggyAdminClient(Map<String, Object> configs) {
        this.config = new IggyKafkaConfig(configs);
        this.connection = new IggyConnection(config);
        this.metrics = new ClientMetrics(config.string("client.id", "iggy-admin"));
        this.requestSensor = metrics.totalAndRate(METRICS_GROUP, "request", "admin requests sent to Iggy");
        this.failedRequestSensor = metrics.totalAndRate(METRICS_GROUP, "failed-request", "admin requests that failed");
        this.latencySensor = metrics.average(METRICS_GROUP, "request-latency", "admin request time in ms");
    }

    /** Mirrors {@link AdminClient#create(Properties)}, so a call site changes only the class name. */
    public static IggyAdminClient create(Properties properties) {
        return new IggyAdminClient(properties);
    }

    /** Mirrors {@link AdminClient#create(Map)}, so a call site changes only the class name. */
    public static IggyAdminClient create(Map<String, Object> configs) {
        return new IggyAdminClient(configs);
    }

    // ---- cluster ----

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        KafkaFuture<Collection<Node>> nodes = future(this::nodes);
        KafkaFuture<Node> controller = nodes.thenApply(ns -> ns.iterator().next());
        KafkaFuture<String> clusterId = future(this::clusterId);
        KafkaFutureImpl<Set<AclOperation>> operations = new KafkaFutureImpl<>();
        operations.complete(null); // Kafka returns null unless the caller asked for them
        return IggyAdminResults.describeCluster(nodes, controller, clusterId, operations);
    }

    /**
     * The cluster's nodes, leader first. A single node is reported at the bootstrap address, since
     * the address a node advertises inside the cluster may not be the one clients reach it on.
     */
    private List<Node> nodes() {
        List<Node> nodes = new ArrayList<>();
        Optional<ClusterMetadata> metadata = clusterMetadata();
        if (metadata.isPresent() && metadata.get().nodes().size() > 1) {
            List<ClusterNode> sorted = new ArrayList<>(metadata.get().nodes());
            sorted.sort(
                    (a, b) -> Boolean.compare(b.role() == ClusterNodeRole.Leader, a.role() == ClusterNodeRole.Leader));
            for (int i = 0; i < sorted.size(); i++) {
                ClusterNode n = sorted.get(i);
                nodes.add(new Node(i, n.ip(), n.endpoints().tcp()));
            }
        }
        if (nodes.isEmpty()) {
            nodes.add(new Node(0, config.host(), config.port()));
        }
        return nodes;
    }

    private String clusterId() {
        return clusterMetadata()
                .map(ClusterMetadata::name)
                .filter(n -> !n.isBlank())
                .orElse("iggy");
    }

    private Optional<ClusterMetadata> clusterMetadata() {
        try {
            return Optional.ofNullable(connection.call(c -> c.system().getClusterMetadata()));
        } catch (KafkaException e) {
            return Optional.empty();
        }
    }

    // ---- topics ----

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return IggyAdminResults.listTopics(future(() -> {
            Map<String, TopicListing> listings = new HashMap<>();
            List<Topic> topics = connection.topics();
            if (!topics.isEmpty()) {
                long stream = connection.streamNumericId();
                for (Topic t : topics) {
                    listings.put(t.name(), new TopicListing(t.name(), topicUuid(stream, t.id()), false));
                }
            }
            return listings;
        }));
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
        if (topics instanceof TopicNameCollection names) {
            Map<String, KafkaFuture<TopicDescription>> futures = new LinkedHashMap<>();
            for (String name : names.topicNames()) {
                futures.put(name, future(() -> describe(connection.describe(name, false))));
            }
            return IggyAdminResults.describeTopics(null, futures);
        }
        Map<Uuid, KafkaFuture<TopicDescription>> futures = new LinkedHashMap<>();
        for (Uuid id : ((TopicIdCollection) topics).topicIds()) {
            futures.put(id, future(() -> describe(connection.describe(topicNamed(id), false))));
        }
        return IggyAdminResults.describeTopics(futures, null);
    }

    private TopicDescription describe(TopicDetails details) {
        Node node = nodes().get(0);
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        List<Partition> sorted = new ArrayList<>(details.partitions());
        sorted.sort((a, b) -> Long.compare(a.id(), b.id()));
        for (Partition p : sorted) {
            partitions.add(new TopicPartitionInfo(p.id().intValue(), node, List.of(node), List.of(node)));
        }
        Uuid id = topicUuid(connection.streamNumericId(), details.id());
        return new TopicDescription(details.name(), false, partitions, Set.of(), id);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        Map<String, KafkaFuture<TopicMetadataAndConfig>> futures = new LinkedHashMap<>();
        for (NewTopic topic : newTopics) {
            futures.put(topic.name(), future(() -> create(topic, options.shouldValidateOnly())));
        }
        return IggyAdminResults.createTopics(futures);
    }

    private TopicMetadataAndConfig create(NewTopic topic, boolean validateOnly) {
        boolean streamExists = true;
        try {
            connection.ensureStream(!validateOnly);
        } catch (UnknownTopicOrPartitionException e) {
            streamExists = false;
        }
        long partitions = topic.numPartitions() > 0
                ? topic.numPartitions()
                : topic.replicasAssignments() != null
                        ? topic.replicasAssignments().size()
                        : config.defaultPartitions();
        Map<String, String> configs = topic.configs() == null ? Map.of() : topic.configs();
        BigInteger expiry = retentionMs(configs.get("retention.ms"));
        BigInteger maxSize = retentionBytes(configs.get("retention.bytes"));
        CompressionAlgorithm compression = compression(configs.get("compression.type"));
        if (streamExists
                && connection
                        .call(c -> c.topics().getTopic(connection.streamId(), TopicId.of(topic.name())))
                        .isPresent()) {
            throw new TopicExistsException("Topic '" + topic.name() + "' already exists.");
        }
        if (validateOnly) {
            return new TopicMetadataAndConfig(Uuid.ZERO_UUID, (int) partitions, 1, new Config(List.of()));
        }
        TopicDetails created = connection.call(c ->
                c.topics().createTopic(connection.streamId(), partitions, compression, expiry, maxSize, topic.name()));
        connection.invalidate(topic.name());
        return new TopicMetadataAndConfig(
                topicUuid(connection.streamNumericId(), created.id()),
                created.partitionsCount().intValue(),
                1,
                configOf(created.messageExpiry(), created.maxTopicSize(), created.compressionAlgorithm()));
    }

    /** Kafka's {@code retention.ms} as Iggy message expiry in microseconds. */
    private static BigInteger retentionMs(String value) {
        if (value == null) {
            return BigInteger.ZERO; // server default
        }
        long ms = parseLong("retention.ms", value);
        return ms < 0 ? U64_MAX : BigInteger.valueOf(ms).multiply(BigInteger.valueOf(1000));
    }

    /** Kafka's {@code retention.bytes} as Iggy's topic size cap. */
    private static BigInteger retentionBytes(String value) {
        if (value == null) {
            return BigInteger.ZERO; // server default
        }
        long bytes = parseLong("retention.bytes", value);
        return bytes < 0 ? U64_MAX : BigInteger.valueOf(bytes);
    }

    private static CompressionAlgorithm compression(String value) {
        if (value == null || value.equalsIgnoreCase("none") || value.equalsIgnoreCase("producer")) {
            return CompressionAlgorithm.None;
        }
        if (value.equalsIgnoreCase("gzip")) {
            return CompressionAlgorithm.Gzip;
        }
        throw new InvalidConfigurationException("Iggy does not support compression.type " + value);
    }

    private static long parseLong(String name, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Invalid value " + value + " for configuration " + name);
        }
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        if (topics instanceof TopicNameCollection names) {
            Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
            for (String name : names.topicNames()) {
                futures.put(name, future(() -> delete(name)));
            }
            return IggyAdminResults.deleteTopics(null, futures);
        }
        Map<Uuid, KafkaFuture<Void>> futures = new LinkedHashMap<>();
        for (Uuid id : ((TopicIdCollection) topics).topicIds()) {
            futures.put(id, future(() -> delete(topicNamed(id))));
        }
        return IggyAdminResults.deleteTopics(futures, null);
    }

    private Void delete(String topic) {
        connection.ensureStream(false);
        connection.call(c -> {
            c.topics().deleteTopic(connection.streamId(), TopicId.of(topic));
            return null;
        });
        connection.invalidate(topic);
        return null;
    }

    @Override
    public CreatePartitionsResult createPartitions(
            Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
        newPartitions.forEach((topic, request) -> futures.put(topic, future(() -> {
            long current = connection.describe(topic, false).partitionsCount();
            int total = request.totalCount();
            if (total <= current) {
                throw new InvalidPartitionsException("Topic currently has " + current
                        + " partitions, which is higher than or equal to the requested " + total + ".");
            }
            if (!options.validateOnly()) {
                connection.call(c -> {
                    c.partitions().createPartitions(connection.streamId(), TopicId.of(topic), total - current);
                    return null;
                });
                connection.invalidate(topic);
            }
            return null;
        })));
        return IggyAdminResults.createPartitions(futures);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        Map<ConfigResource, KafkaFuture<Config>> futures = new LinkedHashMap<>();
        for (ConfigResource resource : resources) {
            futures.put(resource, future(() -> {
                if (resource.type() != ConfigResource.Type.TOPIC) {
                    throw new InvalidRequestException("Iggy has no " + resource.type() + " configuration");
                }
                TopicDetails t = connection.describe(resource.name(), false);
                return configOf(t.messageExpiry(), t.maxTopicSize(), t.compressionAlgorithm());
            }));
        }
        return IggyAdminResults.describeConfigs(futures);
    }

    private static Config configOf(BigInteger expiryMicros, BigInteger maxSizeBytes, CompressionAlgorithm compression) {
        boolean neverExpires = expiryMicros.signum() == 0 || expiryMicros.equals(U64_MAX);
        boolean unlimited = maxSizeBytes.signum() == 0 || maxSizeBytes.equals(U64_MAX);
        return new Config(List.of(
                entry(
                        "retention.ms",
                        neverExpires
                                ? "-1"
                                : expiryMicros.divide(BigInteger.valueOf(1000)).toString(),
                        ConfigEntry.ConfigType.LONG),
                entry("retention.bytes", unlimited ? "-1" : maxSizeBytes.toString(), ConfigEntry.ConfigType.LONG),
                entry("compression.type", compression.name().toLowerCase(), ConfigEntry.ConfigType.STRING)));
    }

    private static ConfigEntry entry(String name, String value, ConfigEntry.ConfigType type) {
        return new ConfigEntry(
                name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, true, List.of(), type, null);
    }

    /** A stable id for an Iggy topic: the stream's numeric id over the topic's. */
    private static Uuid topicUuid(long stream, long topic) {
        return new Uuid(stream, topic);
    }

    private String topicNamed(Uuid id) {
        List<Topic> topics = connection.topics();
        if (!topics.isEmpty() && id.getMostSignificantBits() == connection.streamNumericId()) {
            for (Topic t : topics) {
                if (t.id() == id.getLeastSignificantBits()) {
                    return t.name();
                }
            }
        }
        throw new UnknownTopicIdException("This server does not host this topic ID " + id);
    }

    // ---- consumer groups ----

    /** Iggy groups are per topic; a Kafka group is every Iggy group of that name across the stream. */
    private Map<String, Boolean> groupsWithMembers() {
        Map<String, Boolean> groups = new TreeMap<>();
        for (Topic topic : connection.topics()) {
            List<ConsumerGroup> onTopic = connection.call(
                    c -> c.consumerGroups().getConsumerGroups(connection.streamId(), TopicId.of(topic.name())));
            for (ConsumerGroup g : onTopic) {
                groups.merge(g.name(), g.membersCount() > 0, Boolean::logicalOr);
            }
        }
        return groups;
    }

    /** The group's Iggy groups by topic name, empty when the group exists on no topic. */
    private Map<String, ConsumerGroupDetails> groupByTopic(String groupId) {
        Map<String, ConsumerGroupDetails> found = new LinkedHashMap<>();
        for (Topic topic : connection.topics()) {
            connection
                    .call(c -> c.consumerGroups()
                            .getConsumerGroup(connection.streamId(), TopicId.of(topic.name()), ConsumerId.of(groupId)))
                    .ifPresent(details -> found.put(topic.name(), details));
        }
        return found;
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return IggyAdminResults.listConsumerGroups(future(() -> {
            Collection<Object> listings = new ArrayList<>();
            groupsWithMembers()
                    .forEach((name, active) -> listings.add(IggyAdminResults.consumerGroupListing(name, active)));
            return listings;
        }));
    }

    @Override
    public ListGroupsResult listGroups(ListGroupsOptions options) {
        return IggyAdminResults4.listGroups(future(() -> {
            Collection<Object> listings = new ArrayList<>();
            groupsWithMembers().forEach((name, active) -> listings.add(IggyAdminResults4.groupListing(name, active)));
            return listings;
        }));
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(
            Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        Map<String, KafkaFuture<ConsumerGroupDescription>> futures = new LinkedHashMap<>();
        for (String groupId : groupIds) {
            futures.put(groupId, future(() -> describeGroup(groupId)));
        }
        return IggyAdminResults.describeConsumerGroups(futures);
    }

    private ConsumerGroupDescription describeGroup(String groupId) {
        Map<String, ConsumerGroupDetails> byTopic = groupByTopic(groupId);
        if (byTopic.isEmpty()) {
            throw new GroupIdNotFoundException("Group " + groupId + " not found.");
        }
        // An Iggy member id is the client's id on the server, so one client in several topics is one member.
        Map<Long, Set<TopicPartition>> assignments = new TreeMap<>();
        byTopic.forEach((topic, details) -> {
            for (ConsumerGroupMember member : details.members()) {
                Set<TopicPartition> partitions = assignments.computeIfAbsent(member.id(), id -> new LinkedHashSet<>());
                for (Long p : member.partitions()) {
                    partitions.add(new TopicPartition(topic, p.intValue()));
                }
            }
        });
        Map<Long, String> addresses = clientAddresses();
        List<MemberDescription> members = new ArrayList<>();
        assignments.forEach((id, partitions) -> members.add(IggyAdminResults.memberDescription(
                String.valueOf(id),
                String.valueOf(id),
                addresses.getOrDefault(id, ""),
                new MemberAssignment(partitions))));
        return IggyAdminResults.consumerGroupDescription(groupId, members, "iggy", nodes().get(0));
    }

    private Map<Long, String> clientAddresses() {
        Map<Long, String> addresses = new HashMap<>();
        try {
            for (ClientInfo client : connection.call(c -> c.system().getClients())) {
                addresses.put(client.clientId(), client.address());
            }
        } catch (KafkaException e) {
            // Listing clients needs admin rights; member hosts are then left blank.
        }
        return addresses;
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(
            Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        Map<String, KafkaFuture<Void>> futures = new LinkedHashMap<>();
        for (String groupId : groupIds) {
            futures.put(groupId, future(() -> {
                Map<String, ConsumerGroupDetails> byTopic = groupByTopic(groupId);
                if (byTopic.isEmpty()) {
                    throw new GroupIdNotFoundException("Group " + groupId + " not found.");
                }
                requireNoMembers(groupId, byTopic.values());
                for (String topic : byTopic.keySet()) {
                    connection.call(c -> {
                        c.consumerGroups()
                                .deleteConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId));
                        return null;
                    });
                }
                return null;
            }));
        }
        return IggyAdminResults.deleteConsumerGroups(futures);
    }

    private static void requireNoMembers(String groupId, Collection<ConsumerGroupDetails> groups) {
        for (ConsumerGroupDetails g : groups) {
            if (g.membersCount() > 0) {
                throw new GroupNotEmptyException("Group " + groupId + " has active members.");
            }
        }
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(
            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new LinkedHashMap<>();
        groupSpecs.forEach((groupId, spec) -> futures.put(groupId, future(() -> {
            Collection<TopicPartition> partitions = spec.topicPartitions();
            boolean committedOnly = partitions == null;
            if (partitions == null) {
                partitions = new ArrayList<>();
                for (String topic : groupByTopic(groupId).keySet()) {
                    long count = connection.describe(topic, false).partitionsCount();
                    for (int p = 0; p < count; p++) {
                        partitions.add(new TopicPartition(topic, p));
                    }
                }
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            Consumer group = Consumer.group(ConsumerId.of(groupId));
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata committed = connection.committed(tp, group);
                if (committed != null || !committedOnly) {
                    offsets.put(tp, committed);
                }
            }
            return offsets;
        })));
        return IggyAdminResults.listConsumerGroupOffsets(futures);
    }

    /**
     * Iggy only takes group offsets from the member that owns the partition, so this joins the empty
     * group on each topic, stores the offsets, and leaves again. As in Kafka, the group must have no
     * active members.
     */
    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(
            String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        return IggyAdminResults.alterConsumerGroupOffsets(future(() -> {
            Map<String, List<TopicPartition>> byTopic = new LinkedHashMap<>();
            for (TopicPartition tp : offsets.keySet()) {
                byTopic.computeIfAbsent(tp.topic(), t -> new ArrayList<>()).add(tp);
            }
            List<ConsumerGroupDetails> existing = new ArrayList<>();
            for (String topic : byTopic.keySet()) {
                connection.describe(topic, false);
                connection
                        .call(c -> c.consumerGroups()
                                .getConsumerGroup(connection.streamId(), TopicId.of(topic), ConsumerId.of(groupId)))
                        .ifPresent(existing::add);
            }
            requireNoMembers(groupId, existing);
            Map<TopicPartition, Errors> results = new HashMap<>();
            byTopic.forEach((topic, partitions) -> {
                TopicId topicId = TopicId.of(topic);
                ConsumerId consumerId = ConsumerId.of(groupId);
                connection.ensureGroup(topic, groupId);
                connection.call(c -> {
                    c.consumerGroups().joinConsumerGroup(connection.streamId(), topicId, consumerId);
                    return null;
                });
                try {
                    for (TopicPartition tp : partitions) {
                        long next = offsets.get(tp).offset();
                        if (next <= 0) {
                            // Iggy stores the last consumed offset, so "nothing consumed" cannot be written.
                            results.put(
                                    tp,
                                    Errors.forException(new InvalidRequestException("Iggy cannot store offset 0 for "
                                            + tp + "; delete the group to start over")));
                            continue;
                        }
                        try {
                            connection.call(c -> {
                                c.consumerOffsets()
                                        .storeConsumerOffset(
                                                connection.streamId(),
                                                topicId,
                                                Optional.of((long) tp.partition()),
                                                Consumer.group(consumerId),
                                                BigInteger.valueOf(next - 1));
                                return null;
                            });
                            results.put(tp, Errors.NONE);
                        } catch (KafkaException e) {
                            results.put(tp, Errors.forException(e));
                        }
                    }
                } finally {
                    try {
                        connection.call(c -> {
                            c.consumerGroups().leaveConsumerGroup(connection.streamId(), topicId, consumerId);
                            return null;
                        });
                    } catch (KafkaException e) {
                        // Best effort, and it must not hide a failed store; the server drops the
                        // member when the connection closes.
                    }
                }
            });
            return results;
        }));
    }

    // ---- log offsets ----

    @Override
    public ListOffsetsResult listOffsets(
            Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
        topicPartitionOffsets.forEach((tp, spec) -> futures.put(tp, future(() -> {
            if (spec instanceof OffsetSpec.EarliestSpec) {
                Message first = probe(tp, PollingStrategy.first());
                return new ListOffsetsResultInfo(first == null ? 0 : offset(first), -1, Optional.empty());
            }
            if (spec instanceof OffsetSpec.LatestSpec) {
                Message last = probe(tp, PollingStrategy.last());
                return new ListOffsetsResultInfo(last == null ? 0 : offset(last) + 1, -1, Optional.empty());
            }
            if (spec instanceof OffsetSpec.TimestampSpec timestamp) {
                BigInteger micros = BigInteger.valueOf(IggyAdminResults.timestamp(timestamp))
                        .multiply(BigInteger.valueOf(1000));
                Message found = probe(tp, PollingStrategy.timestamp(micros));
                return found == null
                        ? new ListOffsetsResultInfo(-1, -1, Optional.empty())
                        : new ListOffsetsResultInfo(
                                offset(found), RecordCodec.searchTimestamp(found), Optional.empty());
            }
            throw new UnsupportedVersionException("Iggy supports only earliest, latest and timestamp offset specs");
        })));
        return IggyAdminResults.listOffsets(futures);
    }

    /** The single message a strategy selects, or null when the partition has none. */
    private Message probe(TopicPartition tp, PollingStrategy strategy) {
        connection.describe(tp.topic(), false);
        return connection.probe(tp, strategy);
    }

    private static long offset(Message m) {
        return m.header().offset().longValue();
    }

    // ---- lifecycle ----

    @Override
    public void close(Duration timeout) {
        connection.close();
        metrics.close();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return metrics.all();
    }

    /** Kafka forwards these to client telemetry, which Iggy has no equivalent of. */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {}

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {}

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new IllegalStateException("Client telemetry is not available with Iggy");
    }

    // ---- futures ----

    /** Runs the call now and hands back its outcome as a completed future, as Kafka's results expect. */
    private <T> KafkaFuture<T> future(Callable<T> call) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        long started = System.nanoTime();
        try {
            future.complete(call.call());
        } catch (Throwable t) {
            failedRequestSensor.record(1);
            future.completeExceptionally(translate(t));
        }
        requestSensor.record(1);
        latencySensor.record((System.nanoTime() - started) / 1_000_000.0);
        return future;
    }

    /** Iggy's not-found and already-exists errors as the Kafka exceptions callers test for. */
    private static Throwable translate(Throwable t) {
        if (t instanceof ApiException) {
            return t;
        }
        if (t instanceof KafkaException && t.getCause() instanceof IggyServerException server) {
            IggyErrorCode code = server.getErrorCode();
            if (code != null) {
                switch (code) {
                    case STREAM_ID_NOT_FOUND,
                            STREAM_NAME_NOT_FOUND,
                            TOPIC_ID_NOT_FOUND,
                            TOPIC_NAME_NOT_FOUND,
                            PARTITION_NOT_FOUND -> {
                        return new UnknownTopicOrPartitionException(server.getMessage());
                    }
                    case TOPIC_NAME_ALREADY_EXISTS -> {
                        return new TopicExistsException(server.getMessage());
                    }
                    case CONSUMER_GROUP_ID_NOT_FOUND, CONSUMER_GROUP_NAME_NOT_FOUND -> {
                        return new GroupIdNotFoundException(server.getMessage());
                    }
                    default -> {}
                }
            }
        }
        return t;
    }

    // ---- not supported: nothing in Iggy to map these onto ----

    /** From the kafka-clients 3.x interface, removed in 4.0. */
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        throw IggyKafkaConfig.unsupported("Config changes");
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(
            Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        throw IggyKafkaConfig.unsupported("Config changes");
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        throw IggyKafkaConfig.unsupported("ACLs");
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        throw IggyKafkaConfig.unsupported("ACLs");
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        throw IggyKafkaConfig.unsupported("ACLs");
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(
            Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        throw IggyKafkaConfig.unsupported("Log directories");
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        throw IggyKafkaConfig.unsupported("Log directories");
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(
            Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        throw IggyKafkaConfig.unsupported("Log directories");
    }

    @Override
    public DeleteRecordsResult deleteRecords(
            Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        throw IggyKafkaConfig.unsupported("Record deletion");
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        throw IggyKafkaConfig.unsupported("Delegation tokens");
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        throw IggyKafkaConfig.unsupported("Delegation tokens");
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        throw IggyKafkaConfig.unsupported("Delegation tokens");
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        throw IggyKafkaConfig.unsupported("Delegation tokens");
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(
            String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Deleting single offsets");
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(
            String groupId, RemoveMembersFromConsumerGroupOptions options) {
        throw IggyKafkaConfig.unsupported("Removing group members");
    }

    @Override
    public ElectLeadersResult electLeaders(
            ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        throw IggyKafkaConfig.unsupported("Leader elections");
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
            AlterPartitionReassignmentsOptions options) {
        throw IggyKafkaConfig.unsupported("Partition reassignments");
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        throw IggyKafkaConfig.unsupported("Partition reassignments");
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(
            ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        throw IggyKafkaConfig.unsupported("Quotas");
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(
            Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        throw IggyKafkaConfig.unsupported("Quotas");
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(
            List<String> users, DescribeUserScramCredentialsOptions options) {
        throw IggyKafkaConfig.unsupported("SCRAM credentials");
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(
            List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        throw IggyKafkaConfig.unsupported("SCRAM credentials");
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        throw IggyKafkaConfig.unsupported("Feature flags");
    }

    @Override
    public UpdateFeaturesResult updateFeatures(
            Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        throw IggyKafkaConfig.unsupported("Feature flags");
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        throw IggyKafkaConfig.unsupported("Metadata quorum queries");
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        throw IggyKafkaConfig.unsupported("Broker registration");
    }

    @Override
    public DescribeProducersResult describeProducers(
            Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        throw IggyKafkaConfig.unsupported("Producer state queries");
    }

    @Override
    public DescribeTransactionsResult describeTransactions(
            Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public TerminateTransactionResult forceTerminateTransaction(
            String transactionalId, TerminateTransactionOptions options) {
        throw IggyKafkaConfig.unsupported("Transactions");
    }

    @Override
    public ListConfigResourcesResult listConfigResources(
            Set<ConfigResource.Type> configResourceTypes, ListConfigResourcesOptions options) {
        throw IggyKafkaConfig.unsupported("Config resource listings");
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        throw IggyKafkaConfig.unsupported("Client metrics resources");
    }

    @Override
    public AddRaftVoterResult addRaftVoter(
            int voterId, Uuid voterDirectoryId, Set<RaftVoterEndpoint> endpoints, AddRaftVoterOptions options) {
        throw IggyKafkaConfig.unsupported("Raft voters");
    }

    @Override
    public RemoveRaftVoterResult removeRaftVoter(int voterId, Uuid voterDirectoryId, RemoveRaftVoterOptions options) {
        throw IggyKafkaConfig.unsupported("Raft voters");
    }

    @Override
    public DescribeShareGroupsResult describeShareGroups(
            Collection<String> groupIds, DescribeShareGroupsOptions options) {
        throw IggyKafkaConfig.unsupported("Share groups");
    }

    @Override
    public AlterShareGroupOffsetsResult alterShareGroupOffsets(
            String groupId, Map<TopicPartition, Long> offsets, AlterShareGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Share groups");
    }

    @Override
    public ListShareGroupOffsetsResult listShareGroupOffsets(
            Map<String, ListShareGroupOffsetsSpec> groupSpecs, ListShareGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Share groups");
    }

    @Override
    public DeleteShareGroupOffsetsResult deleteShareGroupOffsets(
            String groupId, Set<String> topics, DeleteShareGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Share groups");
    }

    @Override
    public DeleteShareGroupsResult deleteShareGroups(Collection<String> groupIds, DeleteShareGroupsOptions options) {
        throw IggyKafkaConfig.unsupported("Share groups");
    }

    @Override
    public DescribeStreamsGroupsResult describeStreamsGroups(
            Collection<String> groupIds, DescribeStreamsGroupsOptions options) {
        throw IggyKafkaConfig.unsupported("Streams groups");
    }

    @Override
    public ListStreamsGroupOffsetsResult listStreamsGroupOffsets(
            Map<String, ListStreamsGroupOffsetsSpec> groupSpecs, ListStreamsGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Streams groups");
    }

    @Override
    public AlterStreamsGroupOffsetsResult alterStreamsGroupOffsets(
            String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterStreamsGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Streams groups");
    }

    @Override
    public DeleteStreamsGroupOffsetsResult deleteStreamsGroupOffsets(
            String groupId, Set<TopicPartition> partitions, DeleteStreamsGroupOffsetsOptions options) {
        throw IggyKafkaConfig.unsupported("Streams groups");
    }

    @Override
    public DeleteStreamsGroupsResult deleteStreamsGroups(
            Collection<String> groupIds, DeleteStreamsGroupsOptions options) {
        throw IggyKafkaConfig.unsupported("Streams groups");
    }

    @Override
    public DescribeClassicGroupsResult describeClassicGroups(
            Collection<String> groupIds, DescribeClassicGroupsOptions options) {
        throw IggyKafkaConfig.unsupported("Classic group descriptions");
    }
}
