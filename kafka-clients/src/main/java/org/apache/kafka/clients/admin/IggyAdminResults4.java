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

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * The result types and constructors that only exist from kafka-clients 4.0 or later. Kept apart from
 * {@link IggyAdminResults} so nothing here is touched when a 3.x client jar is on the classpath.
 *
 * <p>Internal to the Iggy Kafka clients. It is public only because it must sit in Kafka's
 * package; it is not part of this library's API and can change without notice.
 */
public final class IggyAdminResults4 {

    private IggyAdminResults4() {}

    public static ListGroupsResult listGroups(KafkaFuture<Collection<Object>> future) {
        return new ListGroupsResult(future);
    }

    public static GroupListing groupListing(String groupId, boolean hasMembers) {
        return new GroupListing(
                groupId,
                Optional.of(GroupType.CONSUMER),
                "consumer",
                Optional.of(hasMembers ? GroupState.STABLE : GroupState.EMPTY));
    }

    /** A member with no instance id, rack, target assignment or epoch, as the 3.x constructor leaves them. */
    public static MemberDescription memberDescription(
            String memberId, String clientId, String host, MemberAssignment assignment) {
        return new MemberDescription(
                memberId,
                Optional.empty(),
                Optional.empty(),
                clientId,
                host,
                assignment,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /** A classic group with no ACL operations or epochs, as the 3.x constructor leaves them. */
    public static ConsumerGroupDescription consumerGroupDescription(
            String groupId, Collection<MemberDescription> members, String assignor, Node coordinator) {
        return new ConsumerGroupDescription(
                groupId,
                false,
                members,
                assignor,
                GroupType.CLASSIC,
                members.isEmpty() ? GroupState.EMPTY : GroupState.STABLE,
                coordinator,
                Set.of(),
                Optional.empty(),
                Optional.empty());
    }
}
