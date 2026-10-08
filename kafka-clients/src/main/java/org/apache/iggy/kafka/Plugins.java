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
import org.apache.kafka.common.utils.Utils;

/** Instantiates and configures serializers and deserializers named in the config, as Kafka does. */
final class Plugins {

    private Plugins() {}

    @SuppressWarnings("unchecked")
    static <T> T create(IggyKafkaConfig config, String key, boolean isKey) {
        Object value = config.raw(key);
        if (value == null) {
            throw new ConfigException("Missing required configuration \"" + key + "\" which has no default value.");
        }
        Object instance;
        try {
            if (value instanceof Class<?> cls) {
                instance = Utils.newInstance(cls);
            } else if (value instanceof String name) {
                instance = Utils.newInstance(name.trim(), Object.class);
            } else {
                instance = value;
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigException(key, value, "Class not found");
        }
        if (instance instanceof org.apache.kafka.common.serialization.Serializer<?> s) {
            s.configure(config.originals(), isKey);
        } else if (instance instanceof org.apache.kafka.common.serialization.Deserializer<?> d) {
            d.configure(config.originals(), isKey);
        } else {
            throw new ConfigException(key, value, "Not a Kafka Serializer or Deserializer");
        }
        return (T) instance;
    }
}
