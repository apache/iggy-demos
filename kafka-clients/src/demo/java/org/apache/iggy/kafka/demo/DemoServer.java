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

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.kafka.IggyKafkaConsumer;
import org.apache.iggy.kafka.IggyKafkaProducer;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A local web page that drives producers and consumers from this library against Iggy and shows
 * what they measure, once a second.
 *
 * <p>Producers write to a set of topics. Consumers are grouped into services: each service is one
 * consumer group reading its own random selection of the topics, as separate applications would.
 * One service can instead read every topic, to compare a catch-all reader with focused ones.
 *
 * <p>Run with {@code mvn -Pdemo compile exec:java}, then open http://localhost:8080. Settings:
 * {@code -Ddemo.bootstrap} (default localhost:18090), {@code -Ddemo.port} (default 8080), and the
 * Iggy password in {@code IGGY_PASSWORD} (default iggy).
 */
public final class DemoServer extends DemoWeb<DemoServer.Settings, DemoServer.Run> {

    static final String STREAM = "kafka";

    DemoServer(String bootstrap, String password) {
        super(bootstrap, password);
    }

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getProperty("demo.bootstrap", "localhost:18090");
        String password = System.getenv().getOrDefault("IGGY_PASSWORD", "iggy");
        serve(new DemoServer(bootstrap, password), "Demo");
    }

    @Override
    String pageResource() {
        return "/demo/index.html";
    }

    @Override
    Settings defaults() {
        return Settings.DEFAULTS;
    }

    @Override
    Settings parse(Map<String, String> form, Settings defaults) {
        return Settings.parse(form, defaults);
    }

    @Override
    Run newRun(Settings settings) {
        return new Run(bootstrap, password, settings);
    }

    /** Everything the page can set. Partitions only take effect when a run starts. */
    record Settings(
            int size,
            int rate,
            int producers,
            int topics,
            int partitions,
            int services,
            int topicsPerService,
            int instances,
            boolean catchAll,
            int skew,
            int linger,
            int pollIdle)
            implements DemoWeb.RunSettings {

        /**
         * Skew is in tenths of the Zipf exponent: 0 sends evenly, 10 is the classic Zipf curve.
         * Linger is the producers' {@code linger.ms} and poll idle the consumers'
         * {@code iggy.poll.idle.ms}, both in milliseconds, at the library's defaults.
         */
        static final Settings DEFAULTS = new Settings(128, 1000, 1, 20, 4, 4, 5, 1, false, 10, 5, 20);

        static Settings parse(Map<String, String> form, Settings defaults) {
            return new Settings(
                    number(form, "size", defaults.size),
                    number(form, "rate", defaults.rate),
                    number(form, "producers", defaults.producers),
                    number(form, "topics", defaults.topics),
                    number(form, "partitions", defaults.partitions),
                    number(form, "services", defaults.services),
                    number(form, "topicsPerService", defaults.topicsPerService),
                    number(form, "instances", defaults.instances),
                    form.containsKey("catchAll") ? form.get("catchAll").equals("true") : defaults.catchAll,
                    number(form, "skew", defaults.skew),
                    number(form, "linger", defaults.linger),
                    number(form, "pollIdle", defaults.pollIdle));
        }

        private static int number(Map<String, String> form, String key, int fallback) {
            String value = form.get(key);
            return value == null ? fallback : Integer.parseInt(value);
        }

        @Override
        public String problem() {
            if (size < 8 || size > 1_000_000) {
                return "Message size must be 8 to 1000000 bytes";
            }
            if (rate < 0) {
                return "Rate must be 0 (unlimited) or more";
            }
            if (producers < 1 || producers > 16 || instances < 1 || instances > 8) {
                return "Producers 1 to 16, instances per service 1 to 8";
            }
            if (topics < 1 || topics > 100 || partitions < 1 || partitions > 64) {
                return "Topics 1 to 100, partitions 1 to 64";
            }
            if (services < 1 || services > 10 || topicsPerService < 1) {
                return "Services 1 to 10, at least 1 topic per service";
            }
            if (skew < 0 || skew > 30) {
                return "Skew 0 to 30 (tenths of the Zipf exponent)";
            }
            if (linger < 0 || linger > 1000 || pollIdle < 0 || pollIdle > 1000) {
                return "Batch wait and poll wait 0 to 1000 ms";
            }
            return null;
        }
    }

    // ---- one run of the workload ----

    /**
     * Producers and services on a set of fresh topics. Each value starts with the send time
     * ({@link System#nanoTime()}), so the consumers, in the same JVM, measure end-to-end latency.
     *
     * <p>Changes from the page are applied by one background worker, {@link #reconcile()}, which
     * brings topics, services, their topic selections and their instances in line with the
     * settings, one step at a time.
     */
    static final class Run implements DemoWeb.DemoRun<Settings> {
        /** Size cap per topic, in MiB. Iggy splits it evenly across the topic's partitions. */
        static final long MAX_TOPIC_MIB = Long.getLong("demo.maxTopicMiB", 1024);
        /**
         * Retention only removes whole sealed segments and keeps at least one per partition, so
         * small segments let the cap take effect and keep the floor small with many topics.
         */
        static final String SEGMENT_SIZE = "4 MiB";

        /** Names the run; topics are this plus -1, -2 and so on, groups this plus -s1, -s2. */
        final String name = "demo-" + System.currentTimeMillis();

        final String bootstrap;
        final String password;
        final int partitions;
        final Properties base = new Properties();
        volatile Settings settings;

        /**
         * The run's current topics, replaced as a whole when topics are added or removed. Producers
         * read it on every send: a single read of a reference, no copying or locking.
         */
        volatile List<String> topics = List.of();

        int nextTopicNumber = 1;
        /**
         * Each topic number's place in the popularity order, shuffled per run so the hot topics are
         * not always the first ones.
         */
        final int[] rank = new int[101];
        /** Picks a topic for each send; rebuilt when the topics or the skew change. */
        volatile TopicPicker picker;

        final Map<String, TopicStats> topicStats = new ConcurrentHashMap<>();

        final List<Producer> producers = new CopyOnWriteArrayList<>();
        final List<Service> services = new CopyOnWriteArrayList<>();

        final LatencySamples latencies = new LatencySamples();
        final LongAdder acked = new LongAdder();
        final LongAdder consumed = new LongAdder();
        final LongAdder consumedBytes = new LongAdder();
        final AtomicLong errors = new AtomicLong();
        /** Messages retention deleted before a service read them: gaps in the offsets it read. */
        final AtomicLong trimmed = new AtomicLong();

        volatile String lastError;
        long lastAcked;
        long lastConsumed;
        long lastBytes;
        long lastTrimmed;
        long lastTick = System.nanoTime();

        final Random random = new Random();
        final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "demo-reconcile");
            t.setDaemon(true);
            return t;
        });
        volatile boolean closing;

        TopicPicker picker(List<String> current) {
            TopicPicker p = picker;
            int skew = settings.skew();
            if (p == null || p.topics() != current || p.skew() != skew) {
                p = TopicPicker.of(current, skew, Comparator.comparingInt(t -> rank[(topicNumber(t) - 1) % 100 + 1]));
                picker = p;
            }
            return p;
        }

        Run(String bootstrap, String password, Settings settings) {
            this.bootstrap = bootstrap;
            this.password = password;
            this.settings = settings;
            this.partitions = settings.partitions();
            List<Integer> order = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                order.add(i);
            }
            java.util.Collections.shuffle(order, random);
            for (int i = 0; i < order.size(); i++) {
                rank[order.get(i)] = i + 1;
            }
            base.put("bootstrap.servers", bootstrap);
            base.put("iggy.password", password);
            try {
                applyTopics(); // topics first, so the producers have somewhere to write
                for (int i = 0; i < settings.producers(); i++) {
                    producers.add(new Producer(i + 1));
                }
                worker.execute(this::reconcile);
            } catch (RuntimeException e) {
                close();
                throw e;
            }
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Settings settings() {
            return settings;
        }

        @Override
        public void apply(Settings next) {
            Settings previous = settings;
            settings = next;
            synchronized (producers) {
                if (next.linger() != previous.linger()) {
                    // linger.ms is fixed when a producer is created, so start new ones.
                    int count = producers.size();
                    for (int i = 0; i < count; i++) {
                        producers.set(i, new Producer(i + 1)).close();
                    }
                }
                while (producers.size() < next.producers()) {
                    producers.add(new Producer(producers.size() + 1));
                }
                while (producers.size() > next.producers()) {
                    producers.remove(producers.size() - 1).close();
                }
            }
            if (next.pollIdle() != previous.pollIdle()) {
                worker.execute(this::restartInstances);
            }
            worker.execute(this::reconcile);
        }

        /**
         * iggy.poll.idle.ms is fixed when a consumer is created, so every instance is closed and
         * the next reconcile adds new ones, one at a time, which read on from the committed offsets.
         */
        private void restartInstances() {
            for (Service service : services) {
                List<Instance> old = List.copyOf(service.instances);
                service.instances.clear();
                old.forEach(Instance::close);
            }
        }

        void fail(String message) {
            errors.incrementAndGet();
            lastError = message;
        }

        // ---- reconciling with the settings ----

        private void reconcile() {
            if (closing) {
                return;
            }
            try {
                applyTopics();
                applyServices();
                for (Service service : services) {
                    service.chooseTopics();
                }
                for (Service service : services) {
                    service.applyInstances();
                }
            } catch (RuntimeException e) {
                fail(e.getMessage());
            }
        }

        /**
         * Adds topics by creating them, then handing them to the producers. Removes the newest
         * topics by first taking them away from the producers, then deleting them, with their
         * data, once in-flight sends have landed. Services drop deleted topics from their
         * selection and pick others.
         */
        private void applyTopics() {
            int wanted = settings.topics();
            List<String> current = topics;
            if (wanted == current.size()) {
                return;
            }
            try (var admin = adminClient()) {
                if (wanted > current.size()) {
                    List<String> grown = new ArrayList<>(current);
                    while (grown.size() < wanted) {
                        String topic = name + "-" + nextTopicNumber++;
                        createTopic(admin, topic);
                        topicStats.put(topic, new TopicStats());
                        grown.add(topic);
                        topics = List.copyOf(grown); // Published as created, so close() deletes them after a failure.
                    }
                } else {
                    List<String> removed = List.copyOf(current.subList(wanted, current.size()));
                    topics = List.copyOf(current.subList(0, wanted));
                    for (Service service : services) {
                        service.chooseTopics();
                    }
                    LockSupport.parkNanos(1_000_000_000L);
                    for (String topic : removed) {
                        deleteTopic(admin, topic);
                        topicStats.remove(topic);
                    }
                }
            }
        }

        private void applyServices() {
            int wanted = settings.services();
            while (services.size() < wanted && !closing) {
                services.add(new Service(services.size() + 1));
            }
            while (services.size() > wanted) {
                services.remove(services.size() - 1).close();
            }
        }

        // ---- producers ----

        /** One producer with its own connection, sending at the per-producer rate. */
        final class Producer {
            final IggyKafkaProducer<byte[], byte[]> producer;
            final Thread thread;
            volatile boolean stopping;

            Producer(int number) {
                Properties p = new Properties();
                p.putAll(base);
                p.put("client.id", "demo-producer-" + number);
                p.put("linger.ms", String.valueOf(settings.linger()));
                // Serializer objects rather than class names: the web server's threads may not see
                // these classes through their context class loader.
                producer = new IggyKafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer());
                thread = new Thread(this::run, "demo-producer-" + number);
                thread.start();
            }

            private void run() {
                long start = System.nanoTime();
                long sent = 0;
                long paced = -1;
                while (!stopping) {
                    long rate = settings.rate(); // per producer, so adding producers adds throughput
                    if (rate != paced) {
                        start = System.nanoTime();
                        sent = 0;
                        paced = rate;
                    }
                    if (rate > 0 && sent >= (long) ((System.nanoTime() - start) * (rate / 1e9))) {
                        LockSupport.parkNanos(200_000);
                        continue;
                    }
                    String topic = picker(topics).pick();
                    TopicStats stats = topicStats.get(topic);
                    byte[] value = new byte[settings.size()];
                    ByteBuffer.wrap(value).putLong(System.nanoTime());
                    try {
                        producer.send(new ProducerRecord<>(topic, value), (metadata, error) -> {
                            if (error == null) {
                                acked.increment();
                                if (stats != null) {
                                    stats.acked.increment();
                                    stats.highestWritten
                                            .computeIfAbsent(metadata.partition(), k -> new AtomicLong(-1))
                                            .accumulateAndGet(metadata.offset(), Math::max);
                                }
                            } else {
                                fail(error.getMessage());
                            }
                        });
                    } catch (RuntimeException e) {
                        if (!stopping) {
                            fail(e.getMessage());
                        }
                    }
                    sent++;
                }
            }

            void close() {
                stopping = true;
                // Closing an Iggy client takes about two seconds, so do it off the caller's thread.
                new Thread(
                                () -> {
                                    join(thread);
                                    producer.close(Duration.ofSeconds(5));
                                },
                                "demo-close")
                        .start();
            }
        }

        // ---- services ----

        /**
         * One application: a consumer group reading its own random selection of topics, or every
         * topic when it is the catch-all. Each message on its topics is read once per service.
         */
        final class Service {
            final int number;
            final String group;
            final List<Instance> instances = new CopyOnWriteArrayList<>();
            /** The topics this service reads; replaced as a whole when the selection changes. */
            volatile List<String> selected = List.of();

            volatile boolean all;
            /** Bumped when the selection changes, so instances resubscribe on their own thread. */
            volatile int version;

            final LatencySamples latencies = new LatencySamples();
            final LongAdder consumed = new LongAdder();
            long lastConsumed;
            /** Highest offset this service has read, per topic and partition. */
            final Map<String, Map<Integer, AtomicLong>> highestRead = new ConcurrentHashMap<>();

            Service(int number) {
                this.number = number;
                this.group = name + "-s" + number;
            }

            /**
             * Keeps as much of the current selection as possible: topics that no longer exist are
             * dropped, and the selection grows or shrinks by random topics to the wanted size.
             */
            synchronized void chooseTopics() {
                List<String> current = topics;
                boolean catchAll = number == 1 && settings.catchAll();
                List<String> next;
                if (catchAll) {
                    next = current;
                } else {
                    Set<String> keep = new LinkedHashSet<>(selected);
                    keep.retainAll(current);
                    int wanted = Math.min(settings.topicsPerService(), current.size());
                    List<String> kept = new ArrayList<>(keep);
                    while (kept.size() > wanted) {
                        kept.remove(random.nextInt(kept.size()));
                    }
                    List<String> spare = new ArrayList<>(current);
                    spare.removeAll(kept);
                    while (kept.size() < wanted && !spare.isEmpty()) {
                        kept.add(spare.remove(random.nextInt(spare.size())));
                    }
                    kept.sort(Comparator.comparingInt(Run.this::topicNumber));
                    next = List.copyOf(kept);
                }
                if (catchAll != all || !next.equals(selected)) {
                    selected = next;
                    all = catchAll;
                    version++;
                }
            }

            /**
             * Adds instances one at a time, each after the previous one has partitions in every
             * topic. Iggy rebalances a group only when membership changes, and counts partitions
             * still being handed over as moved, so instances that join together can leave one of
             * them with nothing. Removing instances needs no pause.
             */
            void applyInstances() {
                int wanted = settings.instances();
                while (instances.size() > wanted) {
                    instances.remove(instances.size() - 1).close();
                }
                while (instances.size() < wanted && !closing) {
                    Instance added = new Instance(this, instances.size() + 1);
                    instances.add(added);
                    int topicsWanted = instances.size() <= partitions ? selected.size() : 0;
                    long deadline = System.nanoTime() + 5_000_000_000L;
                    while (added.topicsOwned() < topicsWanted && System.nanoTime() < deadline && !closing) {
                        LockSupport.parkNanos(100_000_000);
                    }
                }
            }

            /** Messages on this service's topics that it has not read yet. */
            long lag() {
                return unread(selected, topicStats, highestRead);
            }

            /**
             * Offsets in a partition are consecutive, so a jump past the highest offset read means
             * retention deleted the messages in between before this service read them. Offsets at
             * or below it are re-reads after a rebalance and are ignored.
             */
            void noteRead(String topic, int partition, long offset) {
                noteHighestRead(highestRead, trimmed, topic, partition, offset);
            }

            void close() {
                instances.forEach(Instance::close);
            }
        }

        /** One instance of a service: a member of its consumer group. */
        final class Instance {
            final Service service;
            final IggyKafkaConsumer<byte[], byte[]> consumer;
            final Thread thread;
            volatile boolean stopping;
            /** Owned partitions as "topic number:partition", published by the consumer thread. */
            volatile List<String> owned = List.of();

            int subscribedVersion = -1;

            Instance(Service service, int number) {
                this.service = service;
                Properties c = new Properties();
                c.putAll(base);
                c.put("client.id", service.group + "-" + number);
                c.put("group.id", service.group);
                c.put("auto.offset.reset", "earliest");
                c.put("auto.commit.interval.ms", "500");
                c.put("iggy.assignment.refresh.ms", "500");
                c.put("iggy.poll.idle.ms", String.valueOf(settings.pollIdle()));
                consumer = new IggyKafkaConsumer<>(c, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                resubscribe();
                thread = new Thread(this::run, service.group + "-" + number);
                thread.start();
            }

            /** A Kafka consumer is used from one thread, so subscription changes happen here. */
            private void resubscribe() {
                subscribedVersion = service.version;
                if (service.all) {
                    // A pattern, so topics added or removed later are followed without a resubscribe.
                    consumer.subscribe(Pattern.compile(Pattern.quote(name) + "-\\d+"));
                } else {
                    consumer.subscribe(service.selected);
                }
            }

            private void run() {
                Set<TopicPartition> lastAssignment = Set.of();
                while (!stopping) {
                    try {
                        if (subscribedVersion != service.version) {
                            resubscribe();
                        }
                        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                            long latency = System.nanoTime()
                                    - ByteBuffer.wrap(record.value()).getLong();
                            latencies.add(latency);
                            service.latencies.add(latency);
                            service.noteRead(record.topic(), record.partition(), record.offset());
                            service.consumed.increment();
                            consumed.increment();
                            consumedBytes.add(record.value().length);
                        }
                        Set<TopicPartition> assigned = consumer.assignment();
                        if (!assigned.equals(lastAssignment)) {
                            lastAssignment = assigned;
                            owned = assigned.stream()
                                    .sorted(Comparator.comparingInt((TopicPartition tp) -> topicNumber(tp.topic()))
                                            .thenComparingInt(TopicPartition::partition))
                                    .map(tp -> topicNumber(tp.topic()) + ":" + tp.partition())
                                    .toList();
                        }
                    } catch (RuntimeException e) {
                        if (!stopping) {
                            fail(e.getMessage());
                        }
                    }
                }
                consumer.close(); // leaves the group, so Iggy hands its partitions to the others
            }

            long topicsOwned() {
                return owned.stream()
                        .map(o -> o.substring(0, o.indexOf(':')))
                        .distinct()
                        .count();
            }

            void close() {
                stopping = true;
            }
        }

        int topicNumber(String topic) {
            return Integer.parseInt(topic.substring(name.length() + 1));
        }

        // ---- reporting ----

        @Override
        public synchronized String snapshot(String gcPauses) {
            long now = System.nanoTime();
            double seconds = (now - lastTick) / 1e9;
            lastTick = now;

            StringBuilder perTopic = new StringBuilder("[");
            for (String topic : topics) {
                TopicStats stats = topicStats.get(topic);
                if (stats == null) {
                    continue;
                }
                long a = stats.acked.sum();
                String readBy = services.stream()
                        .filter(s -> s.selected.contains(topic))
                        .map(s -> String.valueOf(s.number))
                        .collect(Collectors.joining(","));
                comma(perTopic)
                        .append(String.format(
                                Locale.ROOT,
                                "{\"number\":%d,\"produced\":%.1f,\"readBy\":[%s]}",
                                topicNumber(topic),
                                (a - stats.lastAcked) / seconds,
                                readBy));
                stats.lastAcked = a;
            }
            perTopic.append(']');

            long lag = 0;
            StringBuilder perService = new StringBuilder("[");
            for (Service service : services) {
                long c = service.consumed.sum();
                long serviceLag = service.lag();
                lag += serviceLag;
                double[] pct = service.latencies.drainPercentiles();
                String owners = service.instances.stream()
                        .map(i -> i.owned.stream().map(o -> "\"" + o + "\"").collect(Collectors.joining(",", "[", "]")))
                        .collect(Collectors.joining(",", "[", "]"));
                String selectedNumbers = service.selected.stream()
                        .map(t -> String.valueOf(topicNumber(t)))
                        .collect(Collectors.joining(","));
                comma(perService)
                        .append(String.format(
                                Locale.ROOT,
                                "{\"number\":%d,\"all\":%b,\"topics\":[%s],\"instances\":%d,\"consumed\":%.1f,"
                                        + "\"lag\":%d,\"p50\":%s,\"p99\":%s,\"owners\":%s}",
                                service.number,
                                service.all,
                                selectedNumbers,
                                service.instances.size(),
                                (c - service.lastConsumed) / seconds,
                                serviceLag,
                                num(pct == null ? Double.NaN : pct[0]),
                                num(pct == null ? Double.NaN : pct[1]),
                                owners));
                service.lastConsumed = c;
            }
            perService.append(']');

            long a = acked.sum();
            long c = consumed.sum();
            long b = consumedBytes.sum();
            long t = trimmed.get();
            double[] pct = latencies.drainPercentiles();
            Settings s = settings;
            String json = String.format(
                    Locale.ROOT,
                    "{\"running\":true,\"topic\":\"%s\",\"t\":%d,"
                            + "\"size\":%d,\"rate\":%d,\"producers\":%d,\"topicCount\":%d,\"partitions\":%d,"
                            + "\"services\":%d,\"topicsPerService\":%d,\"instances\":%d,\"catchAll\":%b,\"skew\":%d,"
                            + "\"linger\":%d,\"pollIdle\":%d,\"gc\":%s,"
                            + "\"produced\":%.1f,\"consumed\":%.1f,\"mbps\":%.3f,\"p50\":%s,\"p99\":%s,"
                            + "\"errors\":%d,\"lastError\":%s,\"totalProduced\":%d,\"totalConsumed\":%d,"
                            + "\"lag\":%d,\"trimmed\":%d,\"trimmedRate\":%.1f,\"maxTopicMiB\":%d,"
                            + "\"sendLatencyAvg\":%s,\"fetchLatencyAvg\":%s,"
                            + "\"topicStats\":%s,\"serviceStats\":%s}",
                    name,
                    System.currentTimeMillis(),
                    s.size(),
                    s.rate(),
                    producers.size(),
                    topics.size(),
                    partitions,
                    services.size(),
                    s.topicsPerService(),
                    s.instances(),
                    s.catchAll(),
                    s.skew(),
                    s.linger(),
                    s.pollIdle(),
                    gcPauses,
                    (a - lastAcked) / seconds,
                    (c - lastConsumed) / seconds,
                    (b - lastBytes) / seconds / 1e6,
                    num(pct == null ? Double.NaN : pct[0]),
                    num(pct == null ? Double.NaN : pct[1]),
                    errors.get(),
                    lastError == null ? "null" : DemoJson.quote(lastError),
                    a,
                    c,
                    lag,
                    t,
                    (t - lastTrimmed) / seconds,
                    MAX_TOPIC_MIB,
                    num(average(
                            producers.stream().map(p -> p.producer.metrics()).toList(),
                            "producer-metrics",
                            "request-latency-avg")),
                    num(average(
                            services.stream()
                                    .flatMap(sv -> sv.instances.stream())
                                    .map(i -> i.consumer.metrics())
                                    .toList(),
                            "consumer-fetch-manager-metrics",
                            "fetch-latency-avg")),
                    perTopic,
                    perService);
            lastAcked = a;
            lastConsumed = c;
            lastBytes = b;
            lastTrimmed = t;
            return json;
        }

        private static StringBuilder comma(StringBuilder sb) {
            if (sb.length() > 1) {
                sb.append(',');
            }
            return sb;
        }

        // ---- shutting down ----

        /**
         * Stops every producer and consumer, then deletes the run's topics, with their data and
         * consumer groups, so runs do not pile up on the server's disk. Closing Iggy clients takes
         * a few seconds, so this happens on a thread of its own, which is returned.
         */
        @Override
        public synchronized Thread close() {
            closing = true;
            worker.shutdownNow();
            stopAll();
            Thread cleanup = new Thread(
                    () -> {
                        try {
                            worker.awaitTermination(1, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        List<Producer> stoppingProducers = List.copyOf(producers);
                        List<Instance> stoppingInstances = services.stream()
                                .flatMap(s -> s.instances.stream())
                                .toList();
                        stopAll();
                        producers.clear();
                        services.clear();
                        List<String> toDelete = topics;
                        for (Producer p : stoppingProducers) {
                            join(p.thread);
                            p.producer.close(Duration.ofSeconds(5));
                        }
                        // Each instance closes its consumer, leaving its group, when its loop ends.
                        stoppingInstances.forEach(i -> join(i.thread));
                        try (var admin = adminClient()) {
                            toDelete.forEach(topic -> deleteTopic(admin, topic));
                        } catch (RuntimeException e) {
                            System.err.println("Could not delete the topics of " + name + ": " + e.getMessage());
                        }
                    },
                    "demo-cleanup");
            cleanup.start();
            return cleanup;
        }

        private void stopAll() {
            producers.forEach(p -> p.stopping = true);
            services.forEach(s -> s.instances.forEach(i -> i.stopping = true));
        }

        // ---- talking to Iggy directly ----

        /**
         * Creates one topic with a size cap, so a long run keeps a bounded amount of data once the
         * services have read it. Iggy keeps no data limit by default.
         */
        private void createTopic(IggyTcpClient admin, String topic) {
            StreamId stream = StreamId.of(STREAM);
            if (admin.streams().getStream(stream).isEmpty()) {
                admin.streams().createStream(STREAM);
            }
            admin.topics()
                    .createTopic(
                            stream,
                            (long) partitions,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO, // no message expiry
                            BigInteger.valueOf(MAX_TOPIC_MIB * 1024 * 1024),
                            topic,
                            Map.of("segment_size", HeaderValue.fromString(SEGMENT_SIZE)));
        }

        private void deleteTopic(IggyTcpClient admin, String topic) {
            try {
                admin.topics().deleteTopic(StreamId.of(STREAM), TopicId.of(topic));
                System.out.println("Deleted topic " + topic);
            } catch (RuntimeException e) {
                System.err.println("Could not delete topic " + topic + ": " + e.getMessage());
            }
        }

        private IggyTcpClient adminClient() {
            String[] hostPort = bootstrap.split(":");
            return IggyTcpClient.builder()
                    .host(hostPort[0])
                    .port(Integer.parseInt(hostPort[1]))
                    .credentials("iggy", password)
                    .buildAndLogin();
        }

        /** Mean of one metric across clients, skipping clients that have not recorded it yet. */
        private static double average(
                List<? extends Map<MetricName, ? extends Metric>> all, String group, String name) {
            double sum = 0;
            int n = 0;
            for (Map<MetricName, ? extends Metric> metrics : all) {
                for (Map.Entry<MetricName, ? extends Metric> e : metrics.entrySet()) {
                    if (e.getKey().group().equals(group)
                            && e.getKey().name().equals(name)
                            && e.getValue().metricValue() instanceof Number v
                            && !Double.isNaN(v.doubleValue())) {
                        sum += v.doubleValue();
                        n++;
                    }
                }
            }
            return n == 0 ? Double.NaN : sum / n;
        }
    }
}
