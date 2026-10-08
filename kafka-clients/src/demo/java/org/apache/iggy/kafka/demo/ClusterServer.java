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
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Iggy cluster tab: a three-node Iggy cluster in Docker, each node a container that can be
 * stopped and started on its own. Node 1 serves TCP on the demos' default bootstrap port, so the
 * other tabs run against the cluster once it is up.
 *
 * <p>Every second the page gets each container's state from Docker and the cluster's view of the
 * nodes, their roles included, from the first node whose HTTP API answers.
 */
final class ClusterServer {

    /** One node of the cluster, with the ports it publishes on the host. */
    record Node(
            int id, String name, String container, String ip, int tcp, int http, int quic, int websocket, int replica) {
        int replicaId() {
            return id - 1;
        }
    }

    static final List<Node> NODES = List.of(node(1), node(2), node(3));
    static final String NETWORK = "iggy-demo-cluster";
    static final String SUBNET = "172.29.0.0/24";
    static final String CLUSTER_NAME = "iggy-demo";
    /** Replica authentication is mandatory for a cluster; the secret only has to be the same on every node. */
    private static final String SHARED_SECRET = "iggy-demo-cluster-shared-secret-0123456789";

    private static final Pattern NODE_JSON = Pattern.compile(
            "\\{\"name\":\"([^\"]*)\",\"ip\":\"[^\"]*\",\"endpoints\":\\{[^}]*\\},\"role\":\"([^\"]*)\",\"status\":\"([^\"]*)\"\\}");
    private static final Pattern TOKEN_JSON = Pattern.compile("\"access_token\":\\{\"token\":\"([^\"]+)\"");

    private static Node node(int id) {
        return new Node(
                id,
                "iggy-node-" + id,
                "iggy-demo-node-" + id,
                "172.29.0.1" + id,
                18089 + id,
                12999 + id,
                18079 + id,
                18069 + id,
                19089 + id);
    }

    /** What Docker says about one container. */
    record ContainerState(String status, int exitCode, Instant startedAt) {
        static final ContainerState ABSENT = new ContainerState("absent", 0, null);

        boolean running() {
            return status.equals("running");
        }
    }

    /** The cluster's own view, read from one node. */
    record ClusterView(String from, Map<String, String> roles, Map<String, String> statuses) {}

    private final String docker;
    private final String image;
    private final String password;
    private final List<HttpExchange> listeners = new CopyOnWriteArrayList<>();
    /** Docker commands that change things run one at a time, in the order asked. */
    private final ExecutorService actions = Executors.newSingleThreadExecutor(r -> new Thread(r, "cluster-actions"));

    private final AtomicBoolean busy = new AtomicBoolean();
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(700)).build();
    private final Map<Integer, String> tokens = new ConcurrentHashMap<>();
    private volatile Map<Integer, ContainerState> states = Map.of();
    private volatile ClusterView view;
    private volatile String dockerProblem;
    private volatile String message = "";
    private volatile boolean messageIsError;

    ClusterServer(String docker, String image, String password) {
        this.docker = docker;
        this.image = image;
        this.password = password;
    }

    void mount(HttpServer server, String prefix) {
        server.createContext(prefix + "/", exchange -> page(exchange, prefix + "/"));
        server.createContext(
                prefix + "/start",
                exchange -> action(
                        exchange,
                        form -> startAll(form.getOrDefault("fresh", "false").equals("true"))));
        server.createContext(prefix + "/stop", exchange -> action(exchange, form -> stopAll()));
        server.createContext(prefix + "/node/start", exchange -> action(exchange, form -> startNode(nodeFrom(form))));
        server.createContext(prefix + "/node/stop", exchange -> action(exchange, form -> stopNode(nodeFrom(form))));
        server.createContext(prefix + "/logs", this::logs);
        server.createContext(prefix + "/events", this::events);
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "cluster-tick"))
                .scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    private void page(HttpExchange exchange, String path) throws IOException {
        if (!exchange.getRequestURI().getPath().equals(path)) {
            DemoWeb.send(exchange, 404, "text/plain", "Not found");
            return;
        }
        try (InputStream in = ClusterServer.class.getResourceAsStream("/demo/cluster.html")) {
            DemoWeb.send(
                    exchange, 200, "text/html; charset=utf-8", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private interface Action {
        void run(Map<String, String> form) throws IOException;
    }

    /** Queues a Docker action and answers at once; the outcome reaches the page through the events. */
    private void action(HttpExchange exchange, Action action) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            DemoWeb.send(exchange, 405, "text/plain", "POST only");
            return;
        }
        Map<String, String> form = form(exchange);
        try {
            nodeFrom(form); // rejects a bad id before the action is queued
        } catch (IllegalArgumentException e) {
            DemoWeb.send(exchange, 400, "text/plain", e.getMessage());
            return;
        }
        actions.execute(() -> {
            busy.set(true);
            try {
                action.run(form);
            } catch (IOException | RuntimeException e) {
                say(e.getMessage(), true);
            } finally {
                busy.set(false);
            }
            tick();
        });
        DemoWeb.send(exchange, 202, "text/plain", "queued");
    }

    private static Node nodeFrom(Map<String, String> form) {
        String id = form.get("id");
        if (id == null) {
            return null;
        }
        return NODES.stream()
                .filter(n -> String.valueOf(n.id()).equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No node " + id));
    }

    // ---- actions ----

    private void startAll(boolean fresh) throws IOException {
        say("Starting the cluster" + (fresh ? " from empty data" : ""), false);
        if (fresh) {
            for (Node node : NODES) {
                docker(60, "rm", "-f", "-v", node.container());
            }
        }
        Result network = docker(30, "network", "inspect", NETWORK);
        if (network.code() != 0) {
            check(docker(30, "network", "create", "--subnet", SUBNET, NETWORK), "create the Docker network " + NETWORK);
        }
        for (Node node : NODES) {
            start(node);
        }
        say("Cluster started. Node 1 serves TCP on localhost:" + NODES.get(0).tcp() + ".", false);
    }

    private void startNode(Node node) throws IOException {
        say("Starting " + node.name(), false);
        if (docker(30, "network", "inspect", NETWORK).code() != 0) {
            check(docker(30, "network", "create", "--subnet", SUBNET, NETWORK), "create the Docker network " + NETWORK);
        }
        start(node);
        say(node.name() + " started.", false);
    }

    /** Starts the node's container, creating it on its first start. The data stays between starts. */
    private void start(Node node) throws IOException {
        ContainerState state = inspect().getOrDefault(node.id(), ContainerState.ABSENT);
        if (state.running()) {
            return;
        }
        if (!state.status().equals("absent")) {
            check(docker(60, "start", node.container()), "start " + node.name());
            return;
        }
        List<String> cmd = new ArrayList<>(List.of(
                "run",
                "-d",
                "--name",
                node.container(),
                "--network",
                NETWORK,
                "--ip",
                node.ip(),
                "-p",
                node.tcp() + ":" + node.tcp(),
                "-p",
                node.http() + ":" + node.http(),
                // Iggy uses io_uring, which Docker's default seccomp profile blocks.
                "--security-opt",
                "seccomp=unconfined",
                "--cap-add",
                "SYS_NICE",
                "--ulimit",
                "memlock=-1:-1"));
        Map<String, String> env = new HashMap<>();
        env.put("IGGY_ROOT_USERNAME", "iggy");
        env.put("IGGY_ROOT_PASSWORD", password);
        env.put("IGGY_CLUSTER_ENABLED", "true");
        env.put("IGGY_CLUSTER_NAME", CLUSTER_NAME);
        env.put("IGGY_CLUSTER_AUTH_ENABLED", "true");
        env.put("IGGY_CLUSTER_AUTH_SHARED_SECRET", SHARED_SECRET);
        env.put("IGGY_TCP_ADDRESS", "0.0.0.0:" + node.tcp());
        env.put("IGGY_HTTP_ADDRESS", "0.0.0.0:" + node.http());
        env.put("IGGY_QUIC_ENABLED", "false");
        env.put("IGGY_WEBSOCKET_ENABLED", "false");
        // Docker Desktop's VM has no NUMA topology, which the default "numa:auto" needs.
        env.put("IGGY_SHARDING_CPU_ALLOCATION", "2");
        // The demos cap their topics, and the cleaner runs once a minute by default.
        env.put("IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL", "5s");
        for (Node peer : NODES) {
            String p = "IGGY_CLUSTER_NODES_" + peer.replicaId() + "_";
            env.put(p + "NAME", peer.name());
            env.put(p + "IP", peer.ip());
            // What clients are told to dial: the host side of the published ports.
            env.put(p + "ADVERTISED_ADDRESS", "127.0.0.1");
            env.put(p + "REPLICA_ID", String.valueOf(peer.replicaId()));
            env.put(p + "PORTS_TCP", String.valueOf(peer.tcp()));
            env.put(p + "PORTS_HTTP", String.valueOf(peer.http()));
            env.put(p + "PORTS_QUIC", String.valueOf(peer.quic()));
            env.put(p + "PORTS_WEBSOCKET", String.valueOf(peer.websocket()));
            env.put(p + "PORTS_TCP_REPLICA", String.valueOf(peer.replica()));
        }
        for (var e : env.entrySet()) {
            cmd.add("-e");
            cmd.add(e.getKey() + "=" + e.getValue());
        }
        cmd.add(image);
        cmd.add("--replica-id");
        cmd.add(String.valueOf(node.replicaId()));
        // The first run pulls the image, which can take a while.
        check(docker(600, cmd.toArray(String[]::new)), "run " + node.name());
    }

    private void stopNode(Node node) throws IOException {
        say("Stopping " + node.name(), false);
        check(docker(60, "stop", "-t", "10", node.container()), "stop " + node.name());
        tokens.remove(node.id());
        say(node.name() + " stopped.", false);
    }

    /** Stops every node, keeping the containers and their data for the next start. */
    private void stopAll() throws IOException {
        say("Stopping the cluster", false);
        List<String> cmd = new ArrayList<>(List.of("stop", "-t", "10"));
        NODES.forEach(n -> cmd.add(n.container()));
        docker(90, cmd.toArray(String[]::new)); // missing containers are reported, not fatal
        tokens.clear();
        say("Cluster stopped.", false);
    }

    /** Stops the nodes on the way out, from a thread the caller can wait for. */
    Thread stopCurrent() {
        if (states.values().stream().noneMatch(ContainerState::running)) {
            return null;
        }
        Thread cleanup = new Thread(
                () -> {
                    try {
                        stopAll();
                    } catch (IOException e) {
                        System.err.println("Could not stop the cluster: " + e.getMessage());
                    }
                },
                "cluster-cleanup");
        cleanup.start();
        return cleanup;
    }

    private void logs(HttpExchange exchange) throws IOException {
        Map<String, String> query = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw != null) {
            parse(raw, query);
        }
        Node node;
        try {
            node = nodeFrom(query);
        } catch (IllegalArgumentException e) {
            DemoWeb.send(exchange, 400, "text/plain", e.getMessage());
            return;
        }
        if (node == null) {
            DemoWeb.send(exchange, 400, "text/plain", "Which node? Pass id");
            return;
        }
        Result result = docker(15, "logs", "--tail", "40", node.container());
        // The server colours its log lines for a terminal.
        DemoWeb.send(exchange, 200, "text/plain; charset=utf-8", result.output().replaceAll("\u001B\\[[0-9;]*m", ""));
    }

    // ---- what the page sees ----

    private void say(String text, boolean error) {
        message = text == null ? "" : text;
        messageIsError = error;
    }

    private void tick() {
        try {
            states = inspect();
            dockerProblem = null;
        } catch (IOException e) {
            states = Map.of();
            dockerProblem = e.getMessage();
        }
        view = readView();
        String json = snapshot();
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

    private String snapshot() {
        Map<Integer, ContainerState> states = this.states;
        ClusterView view = this.view;
        StringBuilder json = new StringBuilder("{\"docker\":")
                .append(dockerProblem == null)
                .append(",\"dockerProblem\":")
                .append(DemoJson.quote(dockerProblem == null ? "" : dockerProblem))
                .append(",\"image\":")
                .append(DemoJson.quote(image))
                .append(",\"busy\":")
                .append(busy.get())
                .append(",\"message\":")
                .append(DemoJson.quote(message))
                .append(",\"error\":")
                .append(messageIsError)
                .append(",\"clusterName\":")
                .append(DemoJson.quote(CLUSTER_NAME))
                .append(",\"viewFrom\":")
                .append(view == null ? "null" : DemoJson.quote(view.from()))
                .append(",\"nodes\":[");
        long now = System.currentTimeMillis();
        for (Node node : NODES) {
            ContainerState state = states.getOrDefault(node.id(), ContainerState.ABSENT);
            String role = view == null ? null : view.roles().get(node.name());
            String status = view == null ? null : view.statuses().get(node.name());
            if (node.id() > 1) {
                json.append(',');
            }
            json.append("{\"id\":")
                    .append(node.id())
                    .append(",\"name\":")
                    .append(DemoJson.quote(node.name()))
                    .append(",\"container\":")
                    .append(DemoJson.quote(node.container()))
                    .append(",\"replicaId\":")
                    .append(node.replicaId())
                    .append(",\"tcp\":")
                    .append(node.tcp())
                    .append(",\"http\":")
                    .append(node.http())
                    .append(",\"state\":")
                    .append(DemoJson.quote(state.status()))
                    .append(",\"exitCode\":")
                    .append(state.exitCode())
                    .append(",\"upSeconds\":")
                    .append(
                            state.running() && state.startedAt() != null
                                    ? (now - state.startedAt().toEpochMilli()) / 1000
                                    : "null")
                    .append(",\"role\":")
                    .append(role == null || !state.running() ? "null" : DemoJson.quote(role))
                    .append(",\"status\":")
                    .append(status == null || !state.running() ? "null" : DemoJson.quote(status))
                    .append('}');
        }
        return json.append("]}").toString();
    }

    private void events(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
        exchange.getResponseHeaders().add("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, 0);
        listeners.add(exchange);
    }

    // ---- Docker ----

    record Result(int code, String output) {}

    /** Runs a Docker command and returns its exit code with its output, both streams together. */
    private Result docker(long timeoutSeconds, String... args) throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(docker);
        cmd.addAll(List.of(args));
        Process process;
        try {
            process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        } catch (IOException e) {
            throw new IOException(
                    "Could not run " + docker + ": " + e.getMessage() + ". Set -Ddemo.docker to the Docker command");
        }
        byte[] output;
        try (InputStream in = process.getInputStream()) {
            output = in.readAllBytes();
        }
        try {
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IOException(docker + " " + args[0] + " did not finish in " + timeoutSeconds + " s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + docker);
        }
        return new Result(process.exitValue(), new String(output, StandardCharsets.UTF_8).trim());
    }

    private static void check(Result result, String what) throws IOException {
        if (result.code() != 0) {
            throw new IOException("Could not " + what + ": " + result.output());
        }
    }

    /** Each node's container state, by node id; missing containers are left out. */
    private Map<Integer, ContainerState> inspect() throws IOException {
        List<String> cmd = new ArrayList<>(
                List.of("inspect", "-f", "{{.Name}} {{.State.Status}} {{.State.ExitCode}} {{.State.StartedAt}}"));
        NODES.forEach(n -> cmd.add(n.container()));
        Result result = docker(15, cmd.toArray(String[]::new));
        // Docker's wording differs between versions in case only.
        String output = result.output().toLowerCase(Locale.ROOT);
        if (result.code() != 0 && output.contains("cannot connect to the docker daemon")) {
            throw new IOException("Docker is not running. Start it, then start the cluster.");
        }
        if (result.code() != 0 && !output.contains("no such object")) {
            throw new IOException(result.output());
        }
        Map<Integer, ContainerState> states = new HashMap<>();
        for (String line : result.output().split("\n")) {
            String[] parts = line.trim().split(" ");
            if (parts.length != 4 || !parts[0].startsWith("/")) {
                continue;
            }
            String container = parts[0].substring(1);
            NODES.stream()
                    .filter(n -> n.container().equals(container))
                    .findFirst()
                    .ifPresent(n -> states.put(
                            n.id(), new ContainerState(parts[1], Integer.parseInt(parts[2]), startedAt(parts[3]))));
        }
        return states;
    }

    private static Instant startedAt(String text) {
        try {
            return Instant.parse(text);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ---- the cluster's own view ----

    /** Asks the running nodes, in order, for the cluster metadata, and returns the first answer. */
    private ClusterView readView() {
        for (Node node : NODES) {
            ContainerState state = states.getOrDefault(node.id(), ContainerState.ABSENT);
            if (!state.running()) {
                continue;
            }
            String body = metadata(node);
            if (body == null) {
                continue;
            }
            Map<String, String> roles = new HashMap<>();
            Map<String, String> statuses = new HashMap<>();
            Matcher m = NODE_JSON.matcher(body);
            while (m.find()) {
                roles.put(m.group(1), m.group(2));
                statuses.put(m.group(1), m.group(3));
            }
            if (!roles.isEmpty()) {
                return new ClusterView(node.name(), roles, statuses);
            }
        }
        return null;
    }

    /** The metadata JSON from one node, logging in first when its token is missing or stale. */
    private String metadata(Node node) {
        String token = tokens.get(node.id());
        if (token == null) {
            token = login(node);
            if (token == null) {
                return null;
            }
        }
        String body = get(node, token);
        if (body == null) {
            tokens.remove(node.id());
            token = login(node);
            body = token == null ? null : get(node, token);
        }
        return body;
    }

    private String get(Node node, String token) {
        try {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + node.http() + "/cluster/metadata"))
                    .header("Authorization", "Bearer " + token)
                    .timeout(Duration.ofMillis(700))
                    .GET()
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : null;
        } catch (IOException | RuntimeException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private String login(Node node) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + node.http() + "/users/login"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(700))
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "{\"username\":\"iggy\",\"password\":" + DemoJson.quote(password) + "}"))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return null;
            }
            Matcher m = TOKEN_JSON.matcher(response.body());
            if (!m.find()) {
                return null;
            }
            tokens.put(node.id(), m.group(1));
            return m.group(1);
        } catch (IOException | RuntimeException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    // ---- forms ----

    private static Map<String, String> form(HttpExchange exchange) throws IOException {
        Map<String, String> form = new HashMap<>();
        parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), form);
        return form;
    }

    private static void parse(String encoded, Map<String, String> into) {
        for (String pair : encoded.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                into.put(
                        URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
    }
}
