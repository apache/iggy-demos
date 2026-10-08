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

import org.apache.iggy.kafka.RecordCodec.CodecException;
import org.apache.iggy.kafka.RecordCodec.Decoded;
import org.apache.iggy.kafka.RecordCodec.KafkaRecord;
import org.apache.iggy.kafka.RecordCodec.Window;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks the codec against the rules in the gateway's BRIDGE_MAPPING.md. */
class RecordCodecTest {

    private static final long NOW = 1_759_300_000_000L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static KafkaRecord record(byte[] key, byte[] value, long timestamp, Object... headers) {
        RecordHeaders h = new RecordHeaders();
        for (int i = 0; i < headers.length; i += 2) {
            h.add((String) headers[i], (byte[]) headers[i + 1]);
        }
        return KafkaRecord.of(key, value, h, timestamp);
    }

    private static Message encode(KafkaRecord record) {
        return RecordCodec.toMessage(record, Window.of(List.of(record)));
    }

    private static Set<String> headerNames(Message m) {
        return m.userHeaders().keySet().stream().map(HeaderKey::toString).collect(Collectors.toSet());
    }

    private static Decoded roundTrip(KafkaRecord record) {
        return RecordCodec.fromMessage(encode(record));
    }

    @Test
    void valueOnlyRecordCarriesJustTheVersionHeader() {
        Message m = encode(record(null, b("v"), NOW));
        assertEquals(Set.of("kafka.v"), headerNames(m));
        assertArrayEquals(
                new byte[] {1},
                m.userHeaders().get(HeaderKey.fromString("kafka.v")).value());
        assertEquals(
                HeaderKind.String, m.userHeaders().keySet().iterator().next().kind());
        assertArrayEquals(b("v"), m.payload());
        assertEquals(BigInteger.valueOf(NOW * 1000), m.header().originTimestamp());
    }

    @Test
    void keyAndHeadersUseGatewayNamesAndKinds() {
        Message m = encode(record(b("k"), b("v"), NOW, "trace", b("t1")));
        assertEquals(Set.of("kafka.v", "kafka.key", "kafka.h.trace"), headerNames(m));
        HeaderValue key = m.userHeaders().get(HeaderKey.fromString("kafka.key"));
        assertEquals(HeaderKind.Raw, key.kind());
        assertArrayEquals(b("k"), key.value());

        Decoded d = RecordCodec.fromMessage(m);
        assertArrayEquals(b("k"), d.key());
        assertArrayEquals(b("v"), d.value());
        assertArrayEquals(b("t1"), d.headers().lastHeader("trace").value());
        assertEquals(NOW, d.timestamp());
    }

    @Test
    void tombstoneAndEmptyValueUsePlaceholderAndMarker() {
        Message tombstone = encode(record(b("k"), null, NOW));
        assertArrayEquals(new byte[] {0}, tombstone.payload());
        assertArrayEquals(
                b("null"),
                tombstone.userHeaders().get(HeaderKey.fromString("kafka.value")).value());
        assertNull(RecordCodec.fromMessage(tombstone).value());

        Message empty = encode(record(b("k"), new byte[0], NOW));
        assertArrayEquals(
                b("empty"),
                empty.userHeaders().get(HeaderKey.fromString("kafka.value")).value());
        assertArrayEquals(new byte[0], RecordCodec.fromMessage(empty).value());
    }

    @Test
    void shapesIggyCannotHoldGoIntoTheEnvelope() {
        byte[] longKey = new byte[300];
        List<KafkaRecord> cases = List.of(
                record(new byte[0], b("v"), NOW),
                record(longKey, b("v"), NOW),
                record(b("k"), b("v"), NOW, "nothing", null),
                record(b("k"), b("v"), NOW, "empty", new byte[0]),
                record(b("k"), b("v"), NOW, "big", new byte[256]),
                record(b("k"), b("v"), NOW, "n".repeat(250), b("x")));
        for (KafkaRecord r : cases) {
            Message m = encode(r);
            assertTrue(headerNames(m).contains("kafka.envelope"), "envelope expected");
            assertEquals(Set.of("kafka.v", "kafka.envelope"), headerNames(m));
            Decoded d = RecordCodec.fromMessage(m);
            assertArrayEquals(r.key(), d.key());
            assertArrayEquals(r.value(), d.value());
            List<Header> back = new ArrayList<>();
            d.headers().forEach(back::add);
            assertEquals(r.headers().size(), back.size());
            for (int i = 0; i < back.size(); i++) {
                assertEquals(r.headers().get(i).key(), back.get(i).key());
                assertArrayEquals(r.headers().get(i).value(), back.get(i).value());
            }
        }
    }

    @Test
    void envelopeLayoutMatchesTheDocument() {
        Message m = encode(record(new byte[0], b("vv"), NOW, "h", null));
        byte[] expected = {
            0b11, // key and value present
            0,
            0,
            0,
            0, // key length 0
            2,
            0,
            0,
            0,
            'v',
            'v', // value
            1,
            0,
            0,
            0, // one header
            1,
            0,
            0,
            0,
            'h', // name
            0, // value absent
            0,
            0,
            0,
            0 // value length 0
        };
        assertArrayEquals(expected, m.payload());
    }

    @Test
    void overHeaderBudgetFallsBackToEnvelope() {
        List<Object> headers = new ArrayList<>();
        for (int i = 0; i < 450; i++) {
            headers.add("h" + i);
            headers.add(new byte[255]);
        }
        KafkaRecord r = record(b("k"), b("v"), NOW, headers.toArray());
        Message m = encode(r);
        assertTrue(headerNames(m).contains("kafka.envelope"));
        assertEquals(450, RecordCodec.fromMessage(m).headers().toArray().length);
    }

    @Test
    void repeatedHeaderNameIsRefused() {
        assertThrows(InvalidRecordException.class, () -> record(b("k"), b("v"), NOW, "a", b("1"), "a", b("2")));
    }

    @Test
    void timestampsOutsideTheSendWindowAreClampedAndKept() {
        long far = NOW + 2 * 3600 * 1000; // two hours later, past the ~71.6 minute window
        KafkaRecord early = record(null, b("a"), NOW);
        KafkaRecord late = record(null, b("b"), far);
        Window window = Window.of(List.of(early, late));
        Message lateMessage = RecordCodec.toMessage(late, window);
        assertTrue(headerNames(lateMessage).contains("kafka.ts"));
        assertEquals(
                HeaderKind.Int64,
                lateMessage.userHeaders().get(HeaderKey.fromString("kafka.ts")).kind());
        assertEquals(far, RecordCodec.fromMessage(lateMessage).timestamp());
        assertEquals(
                BigInteger.valueOf(NOW * 1000 + 0xFFFF_FFFFL),
                lateMessage.header().originTimestamp());

        Message epoch = encode(record(null, b("e"), 0));
        assertEquals(
                0L, epoch.userHeaders().get(HeaderKey.fromString("kafka.ts")).asInt64());
        assertEquals(0, RecordCodec.fromMessage(epoch).timestamp());
    }

    private static Message stored(byte[] payload, Map<HeaderKey, HeaderValue> headers, long serverMicros) {
        MessageHeader header = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.valueOf(serverMicros),
                BigInteger.ZERO,
                0L,
                (long) payload.length,
                BigInteger.ZERO);
        Message m = new Message(header, payload, Map.of());
        return headers.isEmpty() ? m : m.withUserHeaders(headers);
    }

    @Test
    void nativeIggyMessagePassesThroughWithNullKey() {
        Map<HeaderKey, HeaderValue> headers = new LinkedHashMap<>();
        headers.put(HeaderKey.fromString("kafka.key"), HeaderValue.fromRaw(b("not-a-key")));
        headers.put(HeaderKey.fromString("kafka.value"), HeaderValue.fromRaw(b("null")));
        headers.put(HeaderKey.fromString("source"), HeaderValue.fromString("sensor"));
        Decoded d = RecordCodec.fromMessage(stored(b("payload"), headers, NOW * 1000));
        assertNull(d.key());
        assertArrayEquals(b("payload"), d.value(), "kafka.value means nothing without kafka.v");
        assertArrayEquals(b("not-a-key"), d.headers().lastHeader("kafka.key").value());
        assertArrayEquals(b("sensor"), d.headers().lastHeader("source").value());
        assertEquals(NOW, d.timestamp(), "falls back to the server timestamp");
    }

    @Test
    void headersComeBackSortedByName() {
        Decoded d = roundTrip(record(b("k"), b("v"), NOW, "zeta", b("1"), "alpha", b("2")));
        List<String> names = new ArrayList<>();
        d.headers().forEach(h -> names.add(h.key()));
        assertEquals(List.of("alpha", "zeta"), names);
    }

    @Test
    void unknownVersionAndBrokenEnvelopeAreRefused() {
        Map<HeaderKey, HeaderValue> v2 = Map.of(HeaderKey.fromString("kafka.v"), HeaderValue.fromRaw(new byte[] {2}));
        assertThrows(CodecException.class, () -> RecordCodec.fromMessage(stored(b("x"), v2, 0)));

        Map<HeaderKey, HeaderValue> env = new LinkedHashMap<>();
        env.put(HeaderKey.fromString("kafka.v"), HeaderValue.fromRaw(new byte[] {1}));
        env.put(HeaderKey.fromString("kafka.envelope"), HeaderValue.fromRaw(new byte[] {1}));
        byte[] trailing = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9};
        assertThrows(CodecException.class, () -> RecordCodec.fromMessage(stored(trailing, env, 0)));
        byte[] hugeCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThrows(CodecException.class, () -> RecordCodec.fromMessage(stored(hugeCount, env, 0)));

        Map<HeaderKey, HeaderValue> badMarker = new LinkedHashMap<>();
        badMarker.put(HeaderKey.fromString("kafka.v"), HeaderValue.fromRaw(new byte[] {1}));
        badMarker.put(HeaderKey.fromString("kafka.value"), HeaderValue.fromRaw(b("maybe")));
        assertThrows(CodecException.class, () -> RecordCodec.fromMessage(stored(b("x"), badMarker, 0)));
    }

    @Test
    void gatewayMessageIgnoresUnprefixedHeaders() {
        Map<HeaderKey, HeaderValue> headers = new LinkedHashMap<>();
        headers.put(HeaderKey.fromString("kafka.v"), HeaderValue.fromRaw(new byte[] {1}));
        headers.put(HeaderKey.fromString("kafka.h.keep"), HeaderValue.fromRaw(b("1")));
        headers.put(HeaderKey.fromString("other"), HeaderValue.fromRaw(b("2")));
        Decoded d = RecordCodec.fromMessage(stored(b("x"), headers, 0));
        assertArrayEquals(b("1"), d.headers().lastHeader("keep").value());
        assertFalse(d.headers().headers("other").iterator().hasNext());
    }
}
