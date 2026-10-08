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

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Translates Kafka client properties into the settings needed to talk to Iggy.
 *
 * <p>Standard Kafka keys are honoured where Iggy has an equivalent. Iggy-only settings use the
 * {@code iggy.} prefix so they can sit in the same property file as the Kafka ones.
 */
final class IggyKafkaConfig {

    static final String STREAM = "iggy.stream";
    static final String USERNAME = "iggy.username";
    static final String PASSWORD = "iggy.password";
    static final String AUTO_CREATE_TOPICS = "iggy.auto.create.topics";
    static final String DEFAULT_PARTITIONS = "iggy.default.partitions";

    private final Map<String, Object> values;

    IggyKafkaConfig(Map<String, ?> configs) {
        this.values = new HashMap<>(configs);
    }

    static Map<String, Object> toMap(Properties properties) {
        Map<String, Object> map = new HashMap<>();
        for (String name : properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        // Non-string values put directly into the Properties (e.g. Serializer instances).
        properties.forEach((k, v) -> map.putIfAbsent(String.valueOf(k), v));
        return map;
    }

    Map<String, Object> originals() {
        return new HashMap<>(values);
    }

    Object raw(String key) {
        return values.get(key);
    }

    String string(String key, String defaultValue) {
        Object value = values.get(key);
        return value == null ? defaultValue : value.toString().trim();
    }

    long longValue(String key, long defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(key, value, "expected a number");
        }
    }

    int intValue(String key, int defaultValue) {
        return Math.toIntExact(longValue(key, defaultValue));
    }

    boolean bool(String key, boolean defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        String text = value.toString().trim();
        if (text.equalsIgnoreCase("true")) {
            return true;
        }
        if (text.equalsIgnoreCase("false")) {
            return false;
        }
        throw new ConfigException(key, value, "expected true or false");
    }

    /** First entry of {@code bootstrap.servers}, as host and port. */
    String host() {
        return bootstrap()[0];
    }

    int port() {
        String port = bootstrap()[1];
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new ConfigException(
                    "bootstrap.servers", values.get("bootstrap.servers"), "port " + port + " is not a number");
        }
    }

    private String[] bootstrap() {
        Object value = values.get("bootstrap.servers");
        if (value == null) {
            throw new ConfigException("Missing required configuration \"bootstrap.servers\"");
        }
        String first;
        if (value instanceof Iterable<?> it) {
            var entries = it.iterator();
            first = entries.hasNext() ? String.valueOf(entries.next()) : "";
        } else {
            first = value.toString().split(",")[0];
        }
        first = first.trim();
        if (first.isEmpty()) {
            throw new ConfigException("bootstrap.servers", value, "must name at least one server");
        }
        int colon = first.lastIndexOf(':');
        if (colon < 0) {
            return new String[] {first, "8090"};
        }
        return new String[] {first.substring(0, colon), first.substring(colon + 1)};
    }

    static UnsupportedOperationException unsupported(String feature) {
        return new UnsupportedOperationException("Not supported by the Iggy Kafka clients: " + feature);
    }

    String stream() {
        return string(STREAM, "kafka");
    }

    boolean autoCreateTopics() {
        return bool(AUTO_CREATE_TOPICS, true);
    }

    long defaultPartitions() {
        return longValue(DEFAULT_PARTITIONS, 1);
    }
}
