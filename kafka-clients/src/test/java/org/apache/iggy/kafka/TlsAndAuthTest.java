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

import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Ulimit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Kafka security settings against an Iggy server that only accepts TLS. */
class TlsAndAuthTest {

    private static final Path TLS = Path.of("src/test/resources/tls").toAbsolutePath();
    private static GenericContainer<?> iggy;
    private static String bootstrap;

    @BeforeAll
    static void startIggy() {
        iggy = new GenericContainer<>(System.getProperty("iggy.image", "apache/iggy:0.9.0"))
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
                .withEnv("IGGY_NODE_ADVERTISED_ADDRESS", "localhost")
                .withEnv("IGGY_SHARDING_CPU_ALLOCATION", "2")
                .withEnv("IGGY_TCP_TLS_ENABLED", "true")
                .withEnv("IGGY_TCP_TLS_SELF_SIGNED", "false")
                .withEnv("IGGY_TCP_TLS_CERT_FILE", "/certs/cert.pem")
                .withEnv("IGGY_TCP_TLS_KEY_FILE", "/certs/key.pem")
                .withCopyFileToContainer(MountableFile.forHostPath(TLS.resolve("cert.pem")), "/certs/cert.pem")
                .withCopyFileToContainer(MountableFile.forHostPath(TLS.resolve("key.pem")), "/certs/key.pem")
                .withExposedPorts(8090)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withCapAdd(Capability.SYS_NICE)
                        .withSecurityOpts(List.of("seccomp=unconfined"))
                        .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                .waitingFor(Wait.forLogMessage(".*Client listener \\(TCP-TLS\\) accepting.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        iggy.start();
        bootstrap = "localhost:" + iggy.getMappedPort(8090);
    }

    @AfterAll
    static void stopIggy() {
        if (iggy != null) {
            iggy.stop();
        }
    }

    private static Properties base() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        p.put("iggy.default.partitions", "1");
        return p;
    }

    private static void sendOne(Properties p) throws Exception {
        try (Producer<String, String> producer = new IggyKafkaProducer<>(p)) {
            String topic = "tls-" + UUID.randomUUID().toString().substring(0, 8);
            assertEquals(
                    0,
                    producer.send(new ProducerRecord<>(topic, "k", "v")).get().offset());
        }
    }

    @Test
    void pemTruststore() throws Exception {
        Properties p = base();
        p.put("security.protocol", "SSL");
        p.put("ssl.truststore.type", "PEM");
        p.put("ssl.truststore.location", TLS.resolve("cert.pem").toString());
        p.put("iggy.password", "iggy");
        sendOne(p);
    }

    @Test
    void inlinePemCertificates() throws Exception {
        Properties p = base();
        p.put("security.protocol", "SSL");
        p.put("ssl.truststore.type", "PEM");
        p.put("ssl.truststore.certificates", Files.readString(TLS.resolve("cert.pem")));
        p.put("iggy.password", "iggy");
        sendOne(p);
    }

    @Test
    void jksAndPkcs12Truststores() throws Exception {
        for (String type : List.of("JKS", "PKCS12")) {
            Properties p = base();
            p.put("security.protocol", "SSL");
            p.put("ssl.truststore.type", type);
            p.put(
                    "ssl.truststore.location",
                    TLS.resolve(type.equals("JKS") ? "truststore.jks" : "truststore.p12")
                            .toString());
            p.put("ssl.truststore.password", "changeit");
            p.put("iggy.password", "iggy");
            sendOne(p);
        }
    }

    @Test
    void saslPlainCredentialsLogIn() throws Exception {
        Properties p = base();
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "PLAIN");
        p.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iggy\" password=\"iggy\";");
        p.put("ssl.truststore.type", "PEM");
        p.put("ssl.truststore.location", TLS.resolve("cert.pem").toString());
        sendOne(p);

        p.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"iggy\" password=\"wrong\";");
        assertThrows(KafkaException.class, () -> sendOne(p), "a wrong password is refused");
    }

    @Test
    void misconfigurationIsReportedUpFront() {
        Properties noPassword = base();
        assertThrows(ConfigException.class, () -> new IggyKafkaProducer<String, String>(noPassword));

        Properties kerberos = base();
        kerberos.put("security.protocol", "SASL_SSL");
        assertThrows(
                ConfigException.class,
                () -> new IggyKafkaProducer<String, String>(kerberos),
                "the default SASL mechanism is GSSAPI, which Iggy cannot use");

        Properties clientCert = base();
        clientCert.put("security.protocol", "SSL");
        clientCert.put("ssl.keystore.location", "/nowhere.jks");
        clientCert.put("iggy.password", "iggy");
        assertThrows(ConfigException.class, () -> new IggyKafkaProducer<String, String>(clientCert));
    }

    @Test
    void plaintextClientCannotTalkToTlsServer() {
        Properties p = base();
        p.put("iggy.password", "iggy");
        assertThrows(KafkaException.class, () -> sendOne(p));
    }
}
