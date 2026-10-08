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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the Admin result objects, whose constructors are package-private or protected in
 * kafka-clients. Kafka's own {@code MockAdminClient} lives in this package for the same reason.
 * Every constructor used here has the same signature in kafka-clients 3.9 and 4.3.
 *
 * <p>Internal to the Iggy Kafka clients. It is public only because it must sit in Kafka's
 * package; it is not part of this library's API and can change without notice.
 */
public final class IggyAdminResults {

    private IggyAdminResults() {}

    public static DescribeClusterResult describeCluster(
            KafkaFuture<Collection<Node>> nodes,
            KafkaFuture<Node> controller,
            KafkaFuture<String> clusterId,
            KafkaFuture<Set<AclOperation>> authorizedOperations) {
        return new DescribeClusterResult(nodes, controller, clusterId, authorizedOperations);
    }

    public static ListTopicsResult listTopics(KafkaFuture<Map<String, TopicListing>> future) {
        return new ListTopicsResult(future);
    }

    public static DescribeTopicsResult describeTopics(
            Map<Uuid, KafkaFuture<TopicDescription>> byId, Map<String, KafkaFuture<TopicDescription>> byName) {
        return new DescribeTopicsResult(byId, byName);
    }

    public static CreateTopicsResult createTopics(
            Map<String, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> futures) {
        return new CreateTopicsResult(futures);
    }

    public static DeleteTopicsResult deleteTopics(
            Map<Uuid, KafkaFuture<Void>> byId, Map<String, KafkaFuture<Void>> byName) {
        return new DeleteTopicsResult(byId, byName);
    }

    public static CreatePartitionsResult createPartitions(Map<String, KafkaFuture<Void>> futures) {
        return new CreatePartitionsResult(futures);
    }

    public static DescribeConfigsResult describeConfigs(Map<ConfigResource, KafkaFuture<Config>> futures) {
        return new DescribeConfigsResult(futures);
    }

    /** The collection holds {@link ConsumerGroupListing} entries, as Kafka's own client returns. */
    public static ListConsumerGroupsResult listConsumerGroups(KafkaFuture<Collection<Object>> future) {
        return new ListConsumerGroupsResult(future);
    }

    public static DescribeConsumerGroupsResult describeConsumerGroups(
            Map<String, KafkaFuture<ConsumerGroupDescription>> futures) {
        return new DescribeConsumerGroupsResult(futures);
    }

    public static DeleteConsumerGroupsResult deleteConsumerGroups(Map<String, KafkaFuture<Void>> futures) {
        return new DeleteConsumerGroupsResult(futures);
    }

    public static ListConsumerGroupOffsetsResult listConsumerGroupOffsets(
            Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> byGroup) {
        Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> keyed = new java.util.HashMap<>();
        byGroup.forEach((group, future) -> keyed.put(CoordinatorKey.byGroupId(group), future));
        return new ListConsumerGroupOffsetsResult(keyed);
    }

    public static AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(
            KafkaFuture<Map<TopicPartition, Errors>> future) {
        return new AlterConsumerGroupOffsetsResult(future);
    }

    public static ListOffsetsResult listOffsets(
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures) {
        return new ListOffsetsResult(futures);
    }

    // ---- consumer groups on kafka-clients 3.x and 4.x ----

    /**
     * True when kafka-clients has the group description constructors that 4.3 does not deprecate,
     * as 4.2 and later do. They are built in {@link IggyAdminResults4}. Older clients get the 3.x
     * constructors through reflection, so nothing here names an API Kafka has marked for removal
     * and the code still compiles once Kafka removes them.
     */
    private static final boolean CURRENT_GROUP_TYPES = hasCurrentGroupTypes();

    private static boolean hasCurrentGroupTypes() {
        try {
            Class<?> groupType = Class.forName("org.apache.kafka.common.GroupType");
            Class<?> groupState = Class.forName("org.apache.kafka.common.GroupState");
            MemberDescription.class.getConstructor(
                    String.class,
                    Optional.class,
                    Optional.class,
                    String.class,
                    String.class,
                    MemberAssignment.class,
                    Optional.class,
                    Optional.class,
                    Optional.class);
            ConsumerGroupDescription.class.getConstructor(
                    String.class,
                    boolean.class,
                    Collection.class,
                    String.class,
                    groupType,
                    groupState,
                    Node.class,
                    Set.class,
                    Optional.class,
                    Optional.class);
            return true;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    public static MemberDescription memberDescription(
            String memberId, String clientId, String host, MemberAssignment assignment) {
        if (CURRENT_GROUP_TYPES) {
            return IggyAdminResults4.memberDescription(memberId, clientId, host, assignment);
        }
        return construct(
                MemberDescription.class,
                new Class<?>[] {String.class, Optional.class, String.class, String.class, MemberAssignment.class},
                memberId,
                Optional.empty(),
                clientId,
                host,
                assignment);
    }

    /** A group that is stable with members and empty without, as Iggy has no rebalancing states. */
    public static ConsumerGroupDescription consumerGroupDescription(
            String groupId, Collection<MemberDescription> members, String assignor, Node coordinator) {
        if (CURRENT_GROUP_TYPES) {
            return IggyAdminResults4.consumerGroupDescription(groupId, members, assignor, coordinator);
        }
        Class<?> state = consumerGroupStateClass();
        return construct(
                ConsumerGroupDescription.class,
                new Class<?>[] {String.class, boolean.class, Collection.class, String.class, state, Node.class},
                groupId,
                false,
                members,
                assignor,
                consumerGroupState(state, !members.isEmpty()),
                coordinator);
    }

    /**
     * An entry for {@link ListConsumerGroupsResult}. Its type, {@code ConsumerGroupListing}, is
     * marked for removal in 4.x and has no replacement there, so it is always built by reflection.
     */
    public static Object consumerGroupListing(String groupId, boolean hasMembers) {
        Class<?> state = consumerGroupStateClass();
        try {
            return Class.forName("org.apache.kafka.clients.admin.ConsumerGroupListing")
                    .getConstructor(String.class, boolean.class, Optional.class)
                    .newInstance(groupId, false, Optional.of(consumerGroupState(state, hasMembers)));
        } catch (ReflectiveOperationException e) {
            throw unusable("ConsumerGroupListing", e);
        }
    }

    private static Class<?> consumerGroupStateClass() {
        try {
            return Class.forName("org.apache.kafka.common.ConsumerGroupState");
        } catch (ClassNotFoundException e) {
            throw unusable("ConsumerGroupState", e);
        }
    }

    private static Object consumerGroupState(Class<?> state, boolean hasMembers) {
        try {
            return state.getField(hasMembers ? "STABLE" : "EMPTY").get(null);
        } catch (ReflectiveOperationException e) {
            throw unusable("ConsumerGroupState", e);
        }
    }

    private static <T> T construct(Class<T> type, Class<?>[] parameters, Object... arguments) {
        try {
            return type.getConstructor(parameters).newInstance(arguments);
        } catch (ReflectiveOperationException e) {
            throw unusable(type.getSimpleName(), e);
        }
    }

    private static IllegalStateException unusable(String type, Exception cause) {
        return new IllegalStateException(
                "The kafka-clients version on the classpath has no " + type + " this client can build", cause);
    }

    /** The timestamp inside an {@link OffsetSpec.TimestampSpec}, whose accessor is package-private. */
    public static long timestamp(OffsetSpec.TimestampSpec spec) {
        return spec.timestamp();
    }
}
