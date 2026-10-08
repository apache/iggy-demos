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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A few metrics under the names and groups Kafka's own clients use, so tools that read
 * {@code metrics()} (Micrometer's Kafka binders, for example) find them where they expect.
 */
final class ClientMetrics implements AutoCloseable {

    private final Metrics metrics = new Metrics();
    private final Map<String, String> tags;

    ClientMetrics(String clientId) {
        this.tags = Map.of("client-id", clientId);
    }

    /** A sensor with a running total and a per-second rate: {@code <name>-total} and {@code <name>-rate}. */
    Sensor totalAndRate(String group, String name, String description) {
        Sensor sensor = metrics.sensor(group + "." + name);
        sensor.add(metrics.metricName(name + "-total", group, "Total " + description, tags), new CumulativeSum());
        sensor.add(metrics.metricName(name + "-rate", group, "Per-second " + description, tags), new Rate());
        return sensor;
    }

    /** A sensor with an average: {@code <name>-avg}. */
    Sensor average(String group, String name, String description) {
        Sensor sensor = metrics.sensor(group + "." + name);
        sensor.add(metrics.metricName(name + "-avg", group, "Average " + description, tags), new Avg());
        return sensor;
    }

    void gauge(String group, String name, String description, Supplier<? extends Number> value) {
        gauge(group, name, description, Map.of(), value);
    }

    /** A gauge with extra tags beyond {@code client-id}, such as Kafka's per-partition metrics carry. */
    MetricName gauge(
            String group,
            String name,
            String description,
            Map<String, String> extraTags,
            Supplier<? extends Number> value) {
        Map<String, String> all = new LinkedHashMap<>(tags);
        all.putAll(extraTags);
        MetricName metricName = metrics.metricName(name, group, description, all);
        metrics.addMetric(metricName, (Gauge<Number>) (config, now) -> value.get());
        return metricName;
    }

    void remove(MetricName metricName) {
        metrics.removeMetric(metricName);
    }

    Map<MetricName, KafkaMetric> all() {
        return metrics.metrics();
    }

    @Override
    public void close() {
        metrics.close();
    }
}
