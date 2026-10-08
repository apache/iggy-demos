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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns Kafka's security settings into what the Iggy SDK takes: TLS on or off, an optional PEM file
 * of trusted certificates, and a username and password.
 *
 * <p>{@code security.protocol} {@code SSL} or {@code SASL_SSL} turns TLS on. The truststore can be
 * PEM, JKS or PKCS12, since the SDK only reads PEM, other types are converted. SASL/PLAIN
 * credentials from {@code sasl.jaas.config} become the Iggy credentials, as in the Iggy Kafka
 * gateway. {@code iggy.username} and {@code iggy.password} take precedence over both.
 */
record Security(boolean tls, File trustedCertificates, String username, String password) {

    /** One PEM file per distinct certificate text, so clients built in a loop do not pile up files. */
    private static final Map<String, File> PEM_FILES = new ConcurrentHashMap<>();

    static Security from(IggyKafkaConfig config) {
        String protocol = config.string("security.protocol", "PLAINTEXT").toUpperCase();
        boolean tls;
        boolean sasl;
        switch (protocol) {
            case "PLAINTEXT" -> {
                tls = false;
                sasl = false;
            }
            case "SSL" -> {
                tls = true;
                sasl = false;
            }
            case "SASL_PLAINTEXT" -> {
                tls = false;
                sasl = true;
            }
            case "SASL_SSL" -> {
                tls = true;
                sasl = true;
            }
            default -> throw new ConfigException("security.protocol", protocol, "unknown security protocol");
        }
        for (String key : List.of("ssl.keystore.location", "ssl.keystore.certificate.chain")) {
            if (config.raw(key) != null) {
                throw new ConfigException(
                        key, config.raw(key), "client certificates are not supported by the Iggy SDK");
            }
        }
        File trusted = tls ? trustedCertificates(config) : null;

        String username = null;
        String password = null;
        if (sasl) {
            String mechanism = config.string("sasl.mechanism", "GSSAPI").toUpperCase();
            if (!mechanism.equals("PLAIN")) {
                throw new ConfigException("sasl.mechanism", mechanism, "only PLAIN maps onto Iggy credentials");
            }
            String jaas = config.string("sasl.jaas.config", null);
            if (jaas == null) {
                throw new ConfigException("Missing sasl.jaas.config for SASL/PLAIN");
            }
            // Kafka's own parser, so quoted, single-quoted and bare values all read as they do there.
            // It refuses anything but exactly one login module, so there is one entry.
            try {
                AppConfigurationEntry entry = JaasContext.loadClientContext(
                                Map.of("sasl.jaas.config", new Password(jaas)))
                        .configurationEntries()
                        .get(0);
                Object user = entry.getOptions().get("username");
                Object pass = entry.getOptions().get("password");
                username = user == null ? null : user.toString();
                password = pass == null ? null : pass.toString();
            } catch (IllegalArgumentException e) {
                throw new ConfigException("sasl.jaas.config", new Password(jaas), e.getMessage());
            }
        }
        username = config.string(IggyKafkaConfig.USERNAME, username != null ? username : "iggy");
        password = config.string(IggyKafkaConfig.PASSWORD, password);
        if (password == null) {
            throw new ConfigException("Missing Iggy password: set iggy.password, or SASL/PLAIN in sasl.jaas.config");
        }
        return new Security(tls, trusted, username, password);
    }

    private static File trustedCertificates(IggyKafkaConfig config) {
        String type = config.string("ssl.truststore.type", "JKS").toUpperCase();
        String inline = config.string("ssl.truststore.certificates", null);
        String location = config.string("ssl.truststore.location", null);
        try {
            if (inline != null) {
                return temporaryPem(inline);
            }
            if (location == null) {
                return null; // The JVM's default trust store.
            }
            if (type.equals("PEM")) {
                return new File(location);
            }
            String password = config.string("ssl.truststore.password", null);
            KeyStore store = KeyStore.getInstance(type);
            try (InputStream in = new FileInputStream(location)) {
                store.load(in, password == null ? null : password.toCharArray());
            }
            StringBuilder pem = new StringBuilder();
            Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
            for (String alias : Collections.list(store.aliases())) {
                Certificate cert = store.getCertificate(alias);
                if (cert != null) {
                    pem.append("-----BEGIN CERTIFICATE-----\n")
                            .append(encoder.encodeToString(cert.getEncoded()))
                            .append("\n-----END CERTIFICATE-----\n");
                }
            }
            if (pem.isEmpty()) {
                throw new ConfigException("ssl.truststore.location", location, "holds no certificates");
            }
            return temporaryPem(pem.toString());
        } catch (IOException | GeneralSecurityException e) {
            throw new ConfigException("ssl.truststore.location", location, "could not be read: " + e.getMessage());
        }
    }

    private static File temporaryPem(String pem) throws IOException {
        File cached = PEM_FILES.get(pem);
        if (cached != null && cached.isFile()) {
            return cached;
        }
        File file = Files.createTempFile("iggy-kafka-truststore", ".pem").toFile();
        file.deleteOnExit();
        Files.writeString(file.toPath(), pem);
        PEM_FILES.put(pem, file);
        return file;
    }
}
