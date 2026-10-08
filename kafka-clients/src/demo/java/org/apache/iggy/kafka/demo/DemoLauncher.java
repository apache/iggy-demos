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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * One page for all the demos, with a tab for each. The Kafka clients demo and the Iggy Java SDK
 * demo run in this JVM, under /kafka/ and /java/. The Iggy Rust SDK demo is a separate process,
 * and its tab shows its page from {@code demo.rustUrl}. The Iggy cluster tab, under /cluster/,
 * runs three Iggy nodes in Docker for the demos to connect to.
 *
 * <p>Run with {@code mvn -Pdemo compile exec:java}, then open http://localhost:8080. Settings:
 * {@code -Ddemo.bootstrap} (default localhost:18090), {@code -Ddemo.port} (default 8080),
 * {@code -Ddemo.rustUrl} (default http://localhost:8081/), {@code -Ddemo.docker} (default docker),
 * {@code -Ddemo.iggyImage} (default apache/iggy:0.9.0), and the Iggy password in
 * {@code IGGY_PASSWORD} (default iggy).
 */
public final class DemoLauncher {

    private DemoLauncher() {}

    public static void main(String[] args) throws IOException {
        int port = Integer.getInteger("demo.port", 8080);
        String bootstrap = System.getProperty("demo.bootstrap", "localhost:18090");
        String configuredRustUrl = System.getProperty("demo.rustUrl", "http://localhost:8081/");
        String rustUrl = configuredRustUrl.endsWith("/") ? configuredRustUrl : configuredRustUrl + "/";
        String password = System.getenv().getOrDefault("IGGY_PASSWORD", "iggy");
        DemoServer kafka = new DemoServer(bootstrap, password);
        IggySdkDemoServer java = new IggySdkDemoServer(bootstrap, password);
        ClusterServer cluster = new ClusterServer(
                System.getProperty("demo.docker", "docker"),
                System.getProperty("demo.iggyImage", "apache/iggy:0.9.0"),
                password);
        HttpServer server = server(port);
        kafka.mount(server, "/kafka");
        java.mount(server, "/java");
        cluster.mount(server, "/cluster");
        server.createContext("/", exchange -> page(exchange, rustUrl));
        server.start();
        // Ctrl-C ends both runs too, so they still get to delete their topics, then stops the cluster's nodes.
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            for (Thread cleanup :
                                    new Thread[] {kafka.stopCurrent(), java.stopCurrent(), cluster.stopCurrent()}) {
                                if (cleanup != null) {
                                    try {
                                        cleanup.join(cleanup.getName().equals("cluster-cleanup") ? 60_000 : 15_000);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                        },
                        "demo-shutdown"));
        System.out.println("Demos running at http://localhost:" + port + " against Iggy at " + bootstrap
                + "; the Iggy Rust SDK tab expects its demo at " + rustUrl + "; the Iggy cluster tab uses Docker");
    }

    /** An HTTP server on the loopback address, as every demo uses. */
    static HttpServer server(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        return server;
    }

    private static void page(HttpExchange exchange, String rustUrl) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (List.of("/kafka", "/java", "/cluster").contains(path)) {
            exchange.getResponseHeaders().add("Location", path + "/");
            exchange.sendResponseHeaders(301, -1);
            exchange.close();
            return;
        }
        if (!path.equals("/")) {
            DemoWeb.send(exchange, 404, "text/plain", "Not found");
            return;
        }
        try (InputStream in = DemoLauncher.class.getResourceAsStream("/demo/demos.html")) {
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8).replace("{{RUST_URL}}", rustUrl);
            DemoWeb.send(exchange, 200, "text/html; charset=utf-8", html);
        }
    }
}
