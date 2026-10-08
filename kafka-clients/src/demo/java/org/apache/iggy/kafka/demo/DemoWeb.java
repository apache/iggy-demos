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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * What both Java demos share: the page, start, update and stop requests, one JSON snapshot a
 * second to every open page, and the pieces of a run that do not depend on the client. Each demo
 * supplies its settings and its run.
 */
abstract class DemoWeb<S extends DemoWeb.RunSettings, R extends DemoWeb.DemoRun<S>> {

    /** What the page can set for one demo. */
    interface RunSettings {
        /** Why the settings cannot be used, or null when they can. */
        String problem();
    }

    /** One run of a demo's workload. */
    interface DemoRun<S> {
        String name();

        S settings();

        void apply(S settings);

        String snapshot(String gcPauses);

        /** Stops the run and returns the thread that is cleaning it up. */
        Thread close();
    }

    final String bootstrap;
    final String password;
    private final List<HttpExchange> listeners = new CopyOnWriteArrayList<>();
    private volatile R run;
    /** Where the page is served, such as /kafka, or empty for the root. */
    private String prefix = "";

    private final GcPauses gc = GcPauses.subscribe();

    DemoWeb(String bootstrap, String password) {
        this.bootstrap = bootstrap;
        this.password = password;
    }

    /** The page, as a class path resource. */
    abstract String pageResource();

    abstract S defaults();

    abstract S parse(Map<String, String> form, S defaults);

    abstract R newRun(S settings);

    /**
     * Serves one demo on its own port until the JVM exits. Ctrl-C ends the run too, so it still gets
     * to delete what it created.
     */
    static void serve(DemoWeb<?, ?> demo, String title) throws IOException {
        int port = Integer.getInteger("demo.port", 8080);
        HttpServer server = DemoLauncher.server(port);
        demo.mount(server, "");
        server.start();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            Thread cleanup = demo.stopCurrent();
                            if (cleanup != null) {
                                try {
                                    cleanup.join(15_000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        },
                        "demo-shutdown"));
        System.out.println(title + " running at http://localhost:" + port + " against Iggy at " + demo.bootstrap);
    }

    /** Serves the page and its requests under {@code prefix}, such as /kafka, and starts the once-a-second tick. */
    void mount(HttpServer server, String prefix) {
        this.prefix = prefix;
        server.createContext(prefix + "/", this::page);
        server.createContext(prefix + "/start", this::startRun);
        server.createContext(prefix + "/update", this::updateRun);
        server.createContext(prefix + "/stop", this::stopRun);
        server.createContext(prefix + "/events", this::events);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    private void page(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals(prefix + "/")) {
            send(exchange, 404, "text/plain", "Not found");
            return;
        }
        try (InputStream in = DemoWeb.class.getResourceAsStream(pageResource())) {
            send(exchange, 200, "text/html; charset=utf-8", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private void startRun(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            send(exchange, 405, "text/plain", "POST only");
            return;
        }
        try {
            S settings = parse(form(exchange), defaults());
            String problem = settings.problem();
            if (problem != null) {
                send(exchange, 400, "text/plain", problem);
                return;
            }
            stopCurrent();
            R started = newRun(settings);
            run = started;
            send(exchange, 200, "text/plain", "started " + started.name());
        } catch (NumberFormatException e) {
            send(exchange, 400, "text/plain", "All settings must be whole numbers");
        } catch (RuntimeException e) {
            send(exchange, 502, "text/plain", "Could not start: " + e.getMessage());
        }
    }

    /** Applies new settings to the run in progress. */
    private void updateRun(HttpExchange exchange) throws IOException {
        R current = run;
        if (current == null) {
            send(exchange, 409, "text/plain", "Not running");
            return;
        }
        try {
            S settings = parse(form(exchange), current.settings());
            String problem = settings.problem();
            if (problem != null) {
                send(exchange, 400, "text/plain", problem);
                return;
            }
            current.apply(settings);
            send(exchange, 200, "text/plain", "updated");
        } catch (NumberFormatException e) {
            send(exchange, 400, "text/plain", "All settings must be whole numbers");
        } catch (RuntimeException e) {
            send(exchange, 502, "text/plain", "Could not apply: " + e.getMessage());
        }
    }

    private void stopRun(HttpExchange exchange) throws IOException {
        stopCurrent();
        send(exchange, 200, "text/plain", "stopped");
    }

    /** Stops the run in progress, if any, and returns the thread that is cleaning it up. */
    Thread stopCurrent() {
        R current = run;
        run = null;
        return current == null ? null : current.close();
    }

    /** Server-sent events: one JSON snapshot per second. */
    private void events(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
        exchange.getResponseHeaders().add("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, 0);
        listeners.add(exchange);
    }

    private void tick() {
        R current = run;
        if (current == null) {
            gc.drainJson(); // pauses while idle belong to no run
        }
        String json = current == null ? "{\"running\":false}" : current.snapshot(gc.drainJson());
        byte[] event = ("data: " + json + "\n\n").getBytes(StandardCharsets.UTF_8);
        for (HttpExchange listener : listeners) {
            try {
                OutputStream out = listener.getResponseBody();
                out.write(event);
                out.flush();
            } catch (IOException e) {
                listeners.remove(listener);
                listener.close();
            }
        }
    }

    private static Map<String, String> form(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> form = new HashMap<>();
        for (String pair : body.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                form.put(
                        URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return form;
    }

    static void send(HttpExchange exchange, int status, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", type);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Waits up to five seconds for a thread to end, keeping the caller's interrupt. */
    static void join(Thread thread) {
        try {
            thread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- pieces of a run ----

    /**
     * Chooses topics with Zipf weights: the topic ranked r gets a share proportional to
     * 1 / r^exponent, so a few topics are hot and the rest form a long tail, as in most real
     * systems. An exponent of 0 shares sends evenly.
     */
    record TopicPicker(List<String> topics, List<String> ordered, int skew, double[] cumulative) {

        /** Orders the topics by popularity, then weights them 1, 1/2^s, 1/3^s... */
        static TopicPicker of(List<String> topics, int skew, Comparator<String> popularity) {
            List<String> ordered = new ArrayList<>(topics);
            ordered.sort(popularity);
            double[] cumulative = new double[ordered.size()];
            double sum = 0;
            for (int i = 0; i < ordered.size(); i++) {
                sum += Math.pow(i + 1, -skew / 10.0);
                cumulative[i] = sum;
            }
            return new TopicPicker(topics, ordered, skew, cumulative);
        }

        String pick() {
            double r = ThreadLocalRandom.current().nextDouble() * cumulative[cumulative.length - 1];
            int i = Arrays.binarySearch(cumulative, r);
            return ordered.get(Math.min(i < 0 ? -i - 1 : i, ordered.size() - 1));
        }
    }

    /** What the producers wrote to one topic. */
    static final class TopicStats {
        final LongAdder acked = new LongAdder();
        final Map<Integer, AtomicLong> highestWritten = new ConcurrentHashMap<>();
        long lastAcked;
    }

    /**
     * Raises the highest offset a service read from a partition. A jump past the next offset means
     * retention deleted messages before the service read them, and they are added to {@code trimmed}.
     */
    static void noteHighestRead(
            Map<String, Map<Integer, AtomicLong>> highestRead,
            AtomicLong trimmed,
            String topic,
            int partition,
            long offset) {
        AtomicLong highest = highestRead
                .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, k -> new AtomicLong(-1));
        long previous = highest.get();
        while (offset > previous) {
            if (highest.compareAndSet(previous, offset)) {
                if (offset > previous + 1) {
                    trimmed.addAndGet(offset - previous - 1);
                }
                return;
            }
            previous = highest.get();
        }
    }

    /** Messages on the selected topics that a service has not read yet. */
    static long unread(
            List<String> selected,
            Map<String, TopicStats> topicStats,
            Map<String, Map<Integer, AtomicLong>> highestRead) {
        long lag = 0;
        for (String topic : selected) {
            TopicStats stats = topicStats.get(topic);
            if (stats == null) {
                continue;
            }
            Map<Integer, AtomicLong> read = highestRead.getOrDefault(topic, Map.of());
            for (var e : stats.highestWritten.entrySet()) {
                AtomicLong r = read.get(e.getKey());
                lag += Math.max(0, e.getValue().get() - (r == null ? -1 : r.get()));
            }
        }
        return lag;
    }

    static String num(double v) {
        return Double.isNaN(v) || Double.isInfinite(v) ? "null" : String.format(Locale.ROOT, "%.3f", v);
    }
}
