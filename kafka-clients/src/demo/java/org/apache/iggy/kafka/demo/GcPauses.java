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

package org.apache.iggy.kafka.demo;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Stop-the-world pauses reported by the JVM's garbage collectors, so the page can mark them on
 * its charts next to the latency they cause. The JVM pushes one notification per collection, so
 * nothing here runs between collections. Pauses stop the whole JVM, so each demo in it subscribes
 * and sees every pause.
 */
final class GcPauses {

    private static final int CAPACITY = 1000;
    private static final List<GcPauses> SUBSCRIBERS = new CopyOnWriteArrayList<>();

    static {
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc instanceof NotificationEmitter emitter) {
                emitter.addNotificationListener(
                        (notification, handback) -> {
                            if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(
                                    notification.getType())) {
                                GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
                                        (CompositeData) notification.getUserData());
                                long duration = info.getGcInfo().getDuration();
                                for (GcPauses subscriber : SUBSCRIBERS) {
                                    if (subscriber.pauses.size() < CAPACITY) {
                                        subscriber.pauses.add(duration);
                                    }
                                }
                            }
                        },
                        null,
                        null);
            }
        }
    }

    /** Pauses not yet handed to a sample, in milliseconds. Bounded so an idle server cannot grow it. */
    private final ConcurrentLinkedQueue<Long> pauses = new ConcurrentLinkedQueue<>();

    private GcPauses() {}

    /** A new subscriber that collects every pause from now on. */
    static GcPauses subscribe() {
        GcPauses subscriber = new GcPauses();
        SUBSCRIBERS.add(subscriber);
        return subscriber;
    }

    /** The pauses since the last call, in milliseconds, as a JSON array such as {@code [45, 12]}. */
    String drainJson() {
        List<Long> out = new ArrayList<>();
        for (Long pause = pauses.poll(); pause != null; pause = pauses.poll()) {
            out.add(pause);
        }
        return out.toString();
    }
}
