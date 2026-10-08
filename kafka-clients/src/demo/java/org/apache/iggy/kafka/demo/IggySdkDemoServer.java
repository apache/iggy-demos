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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendConfirmation;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.topic.CompressionAlgorithm;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * A local web page that drives producers and consumers written directly against the Iggy Java SDK
 * and shows what they measure, once a second.
 *
 * <p>The workload is a microservices event bus. Each stream is a domain and each of its topics an
 * event type. Each service is a consumer group with a home stream: it reads a few topics from its
 * home stream and a few from other streams, and Iggy splits each topic's partitions among the
 * service's consumers.
 *
 * <p>Run with {@code mvn -Pdemo compile exec:java -Ddemo.main=org.apache.iggy.kafka.demo.IggySdkDemoServer}, then open http://localhost:8080. Settings:
 * {@code -Ddemo.bootstrap} (default localhost:18090), {@code -Ddemo.port} (default 8080), and the
 * Iggy password in {@code IGGY_PASSWORD} (default iggy).
 */
public final class IggySdkDemoServer extends DemoWeb<IggySdkDemoServer.Settings, IggySdkDemoServer.Run> {

    IggySdkDemoServer(String bootstrap, String password) {
        super(bootstrap, password);
    }

    public static void main(String[] args) throws IOException {
        String bootstrap = System.getProperty("demo.bootstrap", "localhost:18090");
        String password = System.getenv().getOrDefault("IGGY_PASSWORD", "iggy");
        serve(new IggySdkDemoServer(bootstrap, password), "Iggy SDK demo");
    }

    @Override
    String pageResource() {
        return "/demo/iggy.html";
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
            int streams,
            int topicsPerStream,
            int partitions,
            int services,
            int topicsPerService,
            int consumers,
            int pollInterval,
            int skew,
            int linger,
            int batch)
            implements DemoWeb.RunSettings {

        /**
         * A modest event bus: 5 domains of 6 event types, 10 services each reading 4 of them,
         * 512 byte events at 2,500 a second in all. Consumers are per service. Skew is in tenths
         * of the Zipf exponent: 0 sends evenly, 10 is the classic Zipf curve. Linger is how long,
         * in milliseconds, a producer waits to fill a batch for a topic, and batch the most
         * messages it puts in one request; Iggy's Rust producer waits 5 ms. Poll interval is how
         * long, in milliseconds, a consumer waits before polling a topic again after finding
         * nothing new there; Iggy's Rust consumer polls every 5 ms.
         */
        static final Settings DEFAULTS = new Settings(512, 2500, 1, 5, 6, 4, 10, 4, 1, 5, 10, 5, 1000);
        /** Most topics in a run, across all streams. */
        static final int MAX_TOPICS = 1000;

        static Settings parse(Map<String, String> form, Settings defaults) {
            return new Settings(
                    number(form, "size", defaults.size),
                    number(form, "rate", defaults.rate),
                    number(form, "producers", defaults.producers),
                    number(form, "streams", defaults.streams),
                    number(form, "topicsPerStream", defaults.topicsPerStream),
                    number(form, "partitions", defaults.partitions),
                    number(form, "services", defaults.services),
                    number(form, "topicsPerService", defaults.topicsPerService),
                    number(form, "consumers", defaults.consumers),
                    number(form, "pollInterval", defaults.pollInterval),
                    number(form, "skew", defaults.skew),
                    number(form, "linger", defaults.linger),
                    number(form, "batch", defaults.batch));
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
            if (producers < 1 || producers > 16 || consumers < 1 || consumers > 16) {
                return "Producers 1 to 16, consumers per service 1 to 16";
            }
            if (streams < 1 || topicsPerStream < 1 || streams * topicsPerStream > MAX_TOPICS) {
                return "At least 1 stream and 1 topic per stream, " + MAX_TOPICS + " topics in all";
            }
            if (services < 1 || services > 50 || topicsPerService < 1 || topicsPerService > 50) {
                return "Services 1 to 50, topics per service 1 to 50";
            }
            if (partitions < 1 || partitions > 64) {
                return "Partitions 1 to 64";
            }
            if (skew < 0 || skew > 30) {
                return "Skew 0 to 30 (tenths of the Zipf exponent)";
            }
            if (pollInterval < 0 || pollInterval > 1000) {
                return "Poll interval 0 to 1000 ms";
            }
            if (linger < 0 || linger > 1000 || batch < 1 || batch > 100_000) {
                return "Batch wait 0 to 1000 ms, batch size 1 to 100000 messages";
            }
            return null;
        }
    }

    // ---- one run of the workload ----

    /**
     * Producers and consumers on a set of fresh streams and topics. Each value starts with the send time
     * ({@link System#nanoTime()}), so the consumers, in the same JVM, measure end-to-end latency.
     *
     * <p>Changes from the page are applied by one background worker, {@link #reconcile()}, which
     * brings the streams, topics, services, their topic selections and their consumers in line
     * with the settings, one step at a time.
     */
    static final class Run implements DemoWeb.DemoRun<Settings> {
        /** Size cap per topic, in MiB. Iggy splits it evenly across the topic's partitions. */
        static final long MAX_TOPIC_MIB = Long.getLong("demo.maxTopicMiB", 1024);
        /**
         * Retention only removes whole sealed segments and keeps at least one per partition, so
         * small segments let the cap take effect and keep the floor small with many topics.
         */
        static final String SEGMENT_SIZE = "4 MiB";

        /**
         * Names the run. Its streams are this plus -1, -2 and so on, each holding topics t1, t2 and
         * so on, and its services' consumer groups are this plus -s1, -s2. Inside the demo a topic is known by
         * its stream and topic numbers, such as 3.2 for topic 2 in stream 3.
         */
        final String name = "demo-" + System.currentTimeMillis();

        final String bootstrap;
        final String password;
        final int partitions;
        volatile Settings settings;

        /**
         * The run's current topics, replaced as a whole when topics are added or removed. Producers
         * read it on every send: a single read of a reference, no copying or locking.
         */
        volatile List<String> topics = List.of();
        /** Next topic number in each stream, by stream number. */
        final Map<Integer, Integer> nextTopicNumber = new HashMap<>();
        /** Streams created so far, by number. */
        /** Written by the worker thread and read by the HTTP and cleanup threads, so it is concurrent. */
        final Set<Integer> createdStreams = new ConcurrentSkipListSet<>();
        /**
         * Each topic's place in the popularity order, drawn at random when it is created, so the
         * hot topics are not always the first ones.
         */
        final Map<String, Double> popularity = new ConcurrentHashMap<>();
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
        /** Request times, summed, and request counts, for the average send and poll request. */
        final AtomicLong sendNanos = new AtomicLong();

        final AtomicLong sends = new AtomicLong();
        /** Messages sent in those requests, for the average batch. */
        final AtomicLong sentInRequests = new AtomicLong();

        final AtomicLong pollNanos = new AtomicLong();
        final AtomicLong polls = new AtomicLong();
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
                p = TopicPicker.of(current, skew, Comparator.comparingDouble(t -> popularity.getOrDefault(t, 1.0)));
                picker = p;
            }
            return p;
        }

        Run(String bootstrap, String password, Settings settings) {
            this.bootstrap = bootstrap;
            this.password = password;
            this.settings = settings;
            this.partitions = settings.partitions();
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
            settings = next;
            synchronized (producers) {
                while (producers.size() < next.producers()) {
                    producers.add(new Producer(producers.size() + 1));
                }
                while (producers.size() > next.producers()) {
                    producers.remove(producers.size() - 1).close();
                }
            }
            worker.execute(this::reconcile);
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
                    service.applyConsumers();
                }
            } catch (RuntimeException e) {
                fail(e.getMessage());
            }
        }

        /**
         * Brings the streams and their topics in line with the settings. New streams and topics
         * are created, then handed to the producers. Removed ones are first taken away from the
         * producers, then deleted, with their data, once in-flight sends have landed: the newest
         * topics in each stream, or whole streams when the stream count goes down. Services drop
         * deleted topics from their selection and pick others.
         */
        private void applyTopics() {
            int wantedStreams = settings.streams();
            int perStream = settings.topicsPerStream();
            List<String> current = topics;
            List<String> target = new ArrayList<>();
            List<String> created = new ArrayList<>();
            try (var admin = adminClient()) {
                for (int stream = 1; stream <= wantedStreams; stream++) {
                    int streamNumber = stream;
                    List<String> existing = current.stream()
                            .filter(t -> streamNumber(t) == streamNumber)
                            .toList();
                    target.addAll(existing.subList(0, Math.min(perStream, existing.size())));
                    for (int n = existing.size(); n < perStream; n++) {
                        if (!createdStreams.contains(stream)) {
                            admin.streams().createStream(streamName(stream));
                            createdStreams.add(stream);
                        }
                        int number = nextTopicNumber.merge(stream, 1, Integer::sum);
                        String topic = stream + "." + number;
                        createTopic(admin, topic);
                        topicStats.put(topic, new TopicStats());
                        popularity.put(topic, random.nextDouble());
                        target.add(topic);
                        created.add(topic);
                    }
                }
                List<String> removed =
                        current.stream().filter(t -> !target.contains(t)).toList();
                if (created.isEmpty() && removed.isEmpty()) {
                    return;
                }
                target.sort(BY_KEY);
                topics = List.copyOf(target);
                if (removed.isEmpty()) {
                    return;
                }
                for (Service service : services) {
                    service.chooseTopics();
                }
                LockSupport.parkNanos(1_000_000_000L);
                Set<Integer> keptStreams =
                        target.stream().map(Run::streamNumber).collect(Collectors.toSet());
                for (String topic : removed) {
                    if (keptStreams.contains(streamNumber(topic))) {
                        deleteTopic(admin, topic);
                    }
                    topicStats.remove(topic);
                    popularity.remove(topic);
                }
                for (int stream : List.copyOf(createdStreams)) {
                    if (!keptStreams.contains(stream)) {
                        deleteStream(admin, stream); // its topics and groups go with it
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

        /**
         * One producer with its own connection, sending at the per-producer rate. Messages are
         * collected per topic and sent as one request when the batch is full or its first message
         * has waited the batch wait, as Iggy's Rust producer does. With no wait, the messages that
         * are due are sent straight away. Latency is measured from when a message is made, so
         * time spent waiting in a batch counts.
         */
        final class Producer {
            /** Most messages made in one pass of the loop. */
            static final int ROUND = 1000;
            /** Most bytes of payload in one request, whatever the batch size. */
            static final long MAX_BATCH_BYTES = 1024 * 1024;

            final IggyTcpClient client;
            final Thread thread;
            volatile boolean stopping;

            /** Messages waiting to be sent to one topic. */
            static final class Batch {
                List<Message> messages = new ArrayList<>();
                long bytes;
                long firstNanos;
            }

            Producer(int number) {
                client = adminClient();
                thread = new Thread(this::run, "demo-producer-" + number);
                thread.start();
            }

            private void run() {
                Map<String, Batch> batches = new HashMap<>();
                long start = System.nanoTime();
                long made = 0;
                long paced = -1;
                while (!stopping) {
                    Settings s = settings;
                    long rate = s.rate(); // per producer, so adding producers adds throughput
                    if (rate != paced) {
                        start = System.nanoTime();
                        made = 0;
                        paced = rate;
                    }
                    long due = rate == 0
                            ? ROUND
                            : Math.min(ROUND, (long) ((System.nanoTime() - start) * (rate / 1e9)) - made);
                    boolean busy = false;
                    if (due > 0) {
                        TopicPicker picker = picker(topics);
                        int size = s.size();
                        for (int i = 0; i < due; i++) {
                            byte[] payload = new byte[size];
                            long now = System.nanoTime();
                            ByteBuffer.wrap(payload).putLong(now);
                            String topic = picker.pick();
                            Batch batch = batches.computeIfAbsent(topic, k -> new Batch());
                            if (batch.messages.isEmpty()) {
                                batch.firstNanos = now;
                            }
                            batch.messages.add(message(payload));
                            batch.bytes += size;
                            if (batch.messages.size() >= s.batch() || batch.bytes >= MAX_BATCH_BYTES) {
                                send(topic, batch);
                            }
                        }
                        made += due;
                        busy = true;
                    }
                    long linger = s.linger() * 1_000_000L;
                    long now = System.nanoTime();
                    long wait = 200_000;
                    for (var entry : batches.entrySet()) {
                        Batch batch = entry.getValue();
                        if (batch.messages.isEmpty()) {
                            continue;
                        }
                        long waited = now - batch.firstNanos;
                        if (waited >= linger) {
                            send(entry.getKey(), batch);
                            busy = true;
                        } else {
                            wait = Math.min(wait, linger - waited);
                        }
                    }
                    if (!busy) {
                        LockSupport.parkNanos(wait);
                    }
                }
            }

            private void send(String topic, Batch batch) {
                List<Message> messages = batch.messages;
                batch.messages = new ArrayList<>();
                batch.bytes = 0;
                try {
                    long started = System.nanoTime();
                    SendMessagesResponse response = client.messages()
                            .sendMessages(streamOf(topic), topicOf(topic), Partitioning.balanced(), messages);
                    sendNanos.addAndGet(System.nanoTime() - started);
                    sends.incrementAndGet();
                    sentInRequests.addAndGet(messages.size());
                    acked.add(messages.size());
                    TopicStats stats = topicStats.get(topic);
                    if (stats != null) {
                        stats.acked.add(messages.size());
                        if (response != null && !response.confirmations().isEmpty()) {
                            SendConfirmation confirmation =
                                    response.confirmations().get(0);
                            stats.highestWritten
                                    .computeIfAbsent(confirmation.partitionId().intValue(), k -> new AtomicLong(-1))
                                    .accumulateAndGet(
                                            confirmation.baseOffset().longValue() + messages.size() - 1, Math::max);
                        }
                    }
                } catch (RuntimeException e) {
                    // A topic removed from the run can still be picked by a round already under way.
                    if (!stopping && topicStats.containsKey(topic)) {
                        fail(e.getMessage());
                    }
                }
            }

            void close() {
                stopping = true;
                new Thread(
                                () -> {
                                    join(thread);
                                    client.close();
                                },
                                "demo-close")
                        .start();
            }
        }

        /** A message with a server-generated ID and no user headers. */
        static Message message(byte[] payload) {
            MessageHeader header = new MessageHeader(
                    BigInteger.ZERO,
                    MessageId.serverGenerated(),
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    0L,
                    (long) payload.length,
                    BigInteger.ZERO);
            return new Message(header, payload, Map.of());
        }

        // ---- services ----

        /**
         * One service: a consumer group with a home stream, as a microservice owns a domain. It
         * reads most of its topics from its home stream and about a quarter from other streams,
         * and each message on its topics is read once by the service.
         */
        final class Service {
            final int number;
            final String group;
            final ConsumerId groupId;
            final List<Member> consumers = new CopyOnWriteArrayList<>();
            /** The topics this service reads; replaced as a whole when the selection changes. */
            volatile List<String> selected = List.of();

            volatile int home;
            /** Bumped when the selection changes, so consumers resubscribe on their own thread. */
            volatile int version;

            final LatencySamples latencies = new LatencySamples();
            final LongAdder consumed = new LongAdder();
            long lastConsumed;
            /** Highest offset this service has read, per topic and partition. */
            final Map<String, Map<Integer, AtomicLong>> highestRead = new ConcurrentHashMap<>();

            Service(int number) {
                this.number = number;
                this.group = name + "-s" + number;
                this.groupId = ConsumerId.of(group);
            }

            /**
             * Services take home streams in turn: service 1 stream 1, service 2 stream 2, and
             * round again when there are more services than streams. Keeps as much of the current
             * selection as possible, so changes elsewhere do not reshuffle every service.
             */
            synchronized void chooseTopics() {
                List<String> current = topics;
                int streams = settings.streams();
                int homeStream = (number - 1) % streams + 1;
                int wanted = Math.min(settings.topicsPerService(), current.size());
                List<String> homeTopics = current.stream()
                        .filter(t -> streamNumber(t) == homeStream)
                        .toList();
                List<String> otherTopics = current.stream()
                        .filter(t -> streamNumber(t) != homeStream)
                        .toList();
                int fromOthers = wanted < 2 || otherTopics.isEmpty() ? 0 : Math.max(1, wanted / 4);
                int fromHome = Math.min(wanted - fromOthers, homeTopics.size());
                fromOthers = Math.min(wanted - fromHome, otherTopics.size());
                List<String> next = new ArrayList<>(pick(homeTopics, fromHome));
                next.addAll(pick(otherTopics, fromOthers));
                next.sort(BY_KEY);
                if (homeStream != home || !next.equals(selected)) {
                    selected = List.copyOf(next);
                    home = homeStream;
                    version++;
                }
            }

            /** Up to {@code count} of the candidates, keeping those already selected first. */
            private List<String> pick(List<String> candidates, int count) {
                List<String> kept = new ArrayList<>(
                        candidates.stream().filter(selected::contains).toList());
                while (kept.size() > count) {
                    kept.remove(random.nextInt(kept.size()));
                }
                List<String> spare = new ArrayList<>(candidates);
                spare.removeAll(kept);
                while (kept.size() < count && !spare.isEmpty()) {
                    kept.add(spare.remove(random.nextInt(spare.size())));
                }
                return kept;
            }

            /**
             * Adds consumers one at a time, each after the previous one has partitions in every
             * topic. Iggy rebalances a group only when membership changes, and counts partitions
             * still being handed over as moved, so consumers that join together can leave one of
             * them with nothing. Removing consumers needs no pause.
             */
            void applyConsumers() {
                int wanted = settings.consumers();
                while (consumers.size() > wanted) {
                    consumers.remove(consumers.size() - 1).close();
                }
                while (consumers.size() < wanted && !closing) {
                    Member added = new Member(this, consumers.size() + 1);
                    consumers.add(added);
                    int topicsWanted = consumers.size() <= partitions ? selected.size() : 0;
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
                consumers.forEach(Member::close);
            }
        }

        /**
         * One consumer of a service: a member of the service's group on each of its topics. Iggy
         * consumer groups are per topic, so it joins the group on each topic, then polls the
         * topics in turn with no partition given: the server picks the next partition this member
         * owns, returns the messages after the group's stored offset, and stores the new offset
         * as it does (auto-commit).
         */
        final class Member {
            /** Most messages one poll returns. */
            static final int POLL_COUNT = 5000;
            /** Iggy errors that mean a partition is being handed to another member. */
            static final int PARTITION_NOT_OWNED = 5009;
            /** Iggy errors that mean the topic or group is gone, as when a topic is deleted. */
            static final Set<Integer> GONE = Set.of(1009, 2010, 2011, 3007, 5000, 5003, 5006);

            final Service service;
            final ConsumerId groupId;
            final IggyTcpClient client;
            final Thread thread;
            final Consumer consumer;
            volatile boolean stopping;
            /** Owned partitions as "topic:partition", such as 3.2:1, published by the consumer thread. */
            volatile List<String> owned = List.of();
            /** Topics this consumer has joined the group on. */
            List<String> joined = List.of();

            int subscribedVersion = -1;
            long nextAssignmentCheck;

            Member(Service service, int number) {
                this.service = service;
                this.groupId = service.groupId;
                this.consumer = Consumer.group(groupId);
                client = adminClient();
                thread = new Thread(this::run, service.group + "-" + number);
                thread.start();
            }

            /** Joins the group on newly selected topics and leaves it on dropped ones. */
            private void resubscribe() {
                subscribedVersion = service.version;
                List<String> wanted = service.selected;
                List<String> next = new ArrayList<>();
                for (String topic : joined) {
                    if (wanted.contains(topic)) {
                        next.add(topic);
                    } else {
                        quietly(() ->
                                client.consumerGroups().leaveConsumerGroup(streamOf(topic), topicOf(topic), groupId));
                    }
                }
                for (String topic : wanted) {
                    if (!next.contains(topic) && join(topic)) {
                        next.add(topic);
                    }
                }
                joined = List.copyOf(next);
                nextAssignmentCheck = 0;
            }

            private boolean join(String topic) {
                StreamId stream = streamOf(topic);
                TopicId topicId = topicOf(topic);
                try {
                    if (client.consumerGroups()
                            .getConsumerGroup(stream, topicId, groupId)
                            .isEmpty()) {
                        try {
                            client.consumerGroups().createConsumerGroup(stream, topicId, service.group);
                        } catch (IggyServerException e) {
                            // Another consumer of the service may have created it first.
                            if (client.consumerGroups()
                                    .getConsumerGroup(stream, topicId, groupId)
                                    .isEmpty()) {
                                throw e;
                            }
                        }
                    }
                    client.consumerGroups().joinConsumerGroup(stream, topicId, groupId);
                    return true;
                } catch (IggyServerException e) {
                    if (!GONE.contains(e.getRawErrorCode())) {
                        fail(e.getMessage());
                    }
                    return false;
                }
            }

            /**
             * Polls each topic in turn. A topic that had nothing new is left alone for the poll
             * interval, as Iggy's Rust consumer does, so idle topics do not keep the server busy;
             * a topic that returned messages is polled again straight away.
             */
            private void run() {
                Map<String, Long> idleUntil = new HashMap<>();
                while (!stopping) {
                    try {
                        if (subscribedVersion != service.version) {
                            resubscribe();
                            idleUntil.keySet().retainAll(joined);
                        }
                        if (System.nanoTime() >= nextAssignmentCheck) {
                            publishAssignment();
                        }
                        long interval = settings.pollInterval() * 1_000_000L;
                        long now = System.nanoTime();
                        long wake = now + Math.max(interval, 1_000_000L);
                        boolean any = false;
                        for (String topic : joined) {
                            if (stopping) {
                                break;
                            }
                            Long until = idleUntil.get(topic);
                            if (until != null && until > now) {
                                wake = Math.min(wake, until);
                                continue;
                            }
                            if (poll(topic)) {
                                any = true;
                                idleUntil.remove(topic);
                            } else {
                                idleUntil.put(topic, System.nanoTime() + interval);
                            }
                        }
                        if (!any) {
                            // Nothing new anywhere: sleep until the first topic is due again.
                            LockSupport.parkNanos(Math.max(interval == 0 ? 1_000_000L : 0, wake - System.nanoTime()));
                        }
                    } catch (RuntimeException e) {
                        if (!stopping) {
                            fail(e.getMessage());
                        }
                    }
                }
                // Leaving hands this consumer's partitions to the others straight away.
                for (String topic : joined) {
                    quietly(() -> client.consumerGroups().leaveConsumerGroup(streamOf(topic), topicOf(topic), groupId));
                }
                client.close();
            }

            private boolean poll(String topic) {
                PolledMessages polled;
                long started = System.nanoTime();
                try {
                    polled = client.messages()
                            .pollMessages(
                                    streamOf(topic),
                                    topicOf(topic),
                                    Optional.empty(),
                                    consumer,
                                    PollingStrategy.next(),
                                    (long) POLL_COUNT,
                                    true);
                } catch (IggyServerException e) {
                    if (e.getRawErrorCode() == PARTITION_NOT_OWNED || GONE.contains(e.getRawErrorCode())) {
                        nextAssignmentCheck = 0;
                        return false;
                    }
                    throw e;
                }
                pollNanos.addAndGet(System.nanoTime() - started);
                polls.incrementAndGet();
                long now = System.nanoTime();
                int partition = polled.partitionId().intValue();
                long bytes = 0;
                for (Message message : polled.messages()) {
                    byte[] payload = message.payload();
                    long latency = now - ByteBuffer.wrap(payload).getLong();
                    latencies.add(latency);
                    service.latencies.add(latency);
                    service.noteRead(topic, partition, message.header().offset().longValue());
                    bytes += payload.length;
                }
                consumedBytes.add(bytes);
                int n = polled.messages().size();
                service.consumed.add(n);
                consumed.add(n);
                return n > 0;
            }

            /** Asks Iggy which partitions this member owns in each topic, for the page. */
            private void publishAssignment() {
                nextAssignmentCheck = System.nanoTime() + 500_000_000L;
                List<String> result = new ArrayList<>();
                for (String topic : joined) {
                    try {
                        Optional<ConsumerGroupAssignment> sync =
                                client.consumerGroups().syncConsumerGroup(streamOf(topic), topicOf(topic), groupId);
                        sync.ifPresent(a -> a.partitions().stream().sorted().forEach(p -> result.add(topic + ":" + p)));
                    } catch (IggyServerException e) {
                        if (!GONE.contains(e.getRawErrorCode())) {
                            throw e;
                        }
                    }
                }
                owned = List.copyOf(result);
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

        private static void quietly(Runnable action) {
            try {
                action.run();
            } catch (RuntimeException e) {
                // Best effort: the server drops the member when the connection closes anyway.
            }
        }

        /** Orders topics by stream number, then by topic number within the stream. */
        static final Comparator<String> BY_KEY =
                Comparator.comparingInt(Run::streamNumber).thenComparingInt(Run::topicNumber);

        static int streamNumber(String topic) {
            return Integer.parseInt(topic.substring(0, topic.indexOf('.')));
        }

        static int topicNumber(String topic) {
            return Integer.parseInt(topic.substring(topic.indexOf('.') + 1));
        }

        String streamName(int stream) {
            return name + "-" + stream;
        }

        StreamId streamOf(String topic) {
            return StreamId.of(streamName(streamNumber(topic)));
        }

        static TopicId topicOf(String topic) {
            return TopicId.of("t" + topicNumber(topic));
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
                        .filter(sv -> sv.selected.contains(topic))
                        .map(sv -> String.valueOf(sv.number))
                        .collect(Collectors.joining(","));
                comma(perTopic)
                        .append(String.format(
                                Locale.ROOT,
                                "{\"label\":\"%s\",\"produced\":%.1f,\"readBy\":[%s]}",
                                topic,
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
                String owners = service.consumers.stream()
                        .map(m -> m.owned.stream().map(o -> "\"" + o + "\"").collect(Collectors.joining(",", "[", "]")))
                        .collect(Collectors.joining(",", "[", "]"));
                String selected =
                        service.selected.stream().map(t -> "\"" + t + "\"").collect(Collectors.joining(","));
                comma(perService)
                        .append(String.format(
                                Locale.ROOT,
                                "{\"number\":%d,\"home\":%d,\"topics\":[%s],\"consumers\":%d,\"consumed\":%.1f,"
                                        + "\"lag\":%d,\"p50\":%s,\"p99\":%s,\"owners\":%s}",
                                service.number,
                                service.home,
                                selected,
                                service.consumers.size(),
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
            // Both read the send count, so take the batch average before the request average resets it.
            long requests = sends.get();
            double batchAvg = requests == 0 ? Double.NaN : (double) sentInRequests.getAndSet(0) / requests;
            double sendAvg = averageMillis(sendNanos, sends);
            String json = String.format(
                    Locale.ROOT,
                    "{\"running\":true,\"topic\":\"%s\",\"t\":%d,"
                            + "\"size\":%d,\"rate\":%d,\"producers\":%d,\"streams\":%d,\"topicsPerStream\":%d,\"topicCount\":%d,\"partitions\":%d,"
                            + "\"services\":%d,\"topicsPerService\":%d,\"consumers\":%d,\"pollInterval\":%d,\"skew\":%d,\"linger\":%d,\"batch\":%d,\"gc\":%s,"
                            + "\"produced\":%.1f,\"consumed\":%.1f,\"mbps\":%.3f,\"p50\":%s,\"p99\":%s,"
                            + "\"errors\":%d,\"lastError\":%s,\"totalProduced\":%d,\"totalConsumed\":%d,"
                            + "\"lag\":%d,\"trimmed\":%d,\"trimmedRate\":%.1f,\"maxTopicMiB\":%d,"
                            + "\"sendLatencyAvg\":%s,\"fetchLatencyAvg\":%s,\"batchAvg\":%s,"
                            + "\"maxTopics\":%d,\"topicStats\":%s,\"serviceStats\":%s}",
                    name,
                    System.currentTimeMillis(),
                    s.size(),
                    s.rate(),
                    producers.size(),
                    s.streams(),
                    s.topicsPerStream(),
                    topics.size(),
                    partitions,
                    services.size(),
                    s.topicsPerService(),
                    s.consumers(),
                    s.pollInterval(),
                    s.skew(),
                    s.linger(),
                    s.batch(),
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
                    num(sendAvg),
                    num(averageMillis(pollNanos, polls)),
                    num(batchAvg),
                    Settings.MAX_TOPICS,
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
                        List<Member> stoppingConsumers = services.stream()
                                .flatMap(sv -> sv.consumers.stream())
                                .toList();
                        stopAll();
                        producers.clear();
                        services.clear();
                        List<Integer> toDelete = List.copyOf(createdStreams);
                        for (Producer p : stoppingProducers) {
                            join(p.thread);
                            p.client.close();
                        }
                        // Each consumer leaves its group and closes its connection when its loop ends.
                        stoppingConsumers.forEach(m -> join(m.thread));
                        try (var admin = adminClient()) {
                            toDelete.forEach(stream -> deleteStream(admin, stream));
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
            services.forEach(sv -> sv.consumers.forEach(m -> m.stopping = true));
        }

        // ---- talking to Iggy directly ----

        /**
         * Creates one topic with a size cap, so a long run keeps a bounded amount of data once the
         * services have read it. Iggy keeps no data limit by default.
         */
        private void createTopic(IggyTcpClient admin, String topic) {
            admin.topics()
                    .createTopic(
                            streamOf(topic),
                            (long) partitions,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO, // no message expiry
                            BigInteger.valueOf(MAX_TOPIC_MIB * 1024 * 1024),
                            "t" + topicNumber(topic),
                            Map.of("segment_size", HeaderValue.fromString(SEGMENT_SIZE)));
        }

        private void deleteTopic(IggyTcpClient admin, String topic) {
            try {
                admin.topics().deleteTopic(streamOf(topic), topicOf(topic));
                System.out.println("Deleted topic " + topic);
            } catch (RuntimeException e) {
                System.err.println("Could not delete topic " + topic + ": " + e.getMessage());
            }
        }

        private void deleteStream(IggyTcpClient admin, int stream) {
            try {
                admin.streams().deleteStream(StreamId.of(streamName(stream)));
                createdStreams.remove(stream);
                System.out.println("Deleted stream " + streamName(stream));
            } catch (RuntimeException e) {
                System.err.println("Could not delete stream " + streamName(stream) + ": " + e.getMessage());
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

        /** Mean request time in milliseconds since the last call, then starts again. */
        private static double averageMillis(AtomicLong nanos, AtomicLong count) {
            long n = count.getAndSet(0);
            long total = nanos.getAndSet(0);
            return n == 0 ? Double.NaN : total / 1e6 / n;
        }
    }
}
