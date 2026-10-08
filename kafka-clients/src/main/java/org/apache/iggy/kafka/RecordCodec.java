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

import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps Kafka records onto Iggy messages and back, in the format the Iggy Kafka gateway uses
 * ({@code gateways/kafka/docs/BRIDGE_MAPPING.md}, mapping version 1). Records written here can be
 * read through the gateway, and the reverse.
 *
 * <p>The value is the Iggy payload. The key goes in {@value #KEY_HEADER}, and each Kafka header in
 * {@value #HEADER_PREFIX}{@code <name>}. Every message carries {@value #VERSION_HEADER}, which is how
 * the read side tells these messages from ones a native Iggy client wrote. Records Iggy cannot hold
 * natively (an empty or long key, a null, empty or long header value) are packed into an envelope
 * in the payload instead.
 */
final class RecordCodec {

    static final String VERSION_HEADER = "kafka.v";
    static final byte MAPPING_VERSION = 1;
    static final String KEY_HEADER = "kafka.key";
    static final String VALUE_MARKER_HEADER = "kafka.value";
    static final String TIMESTAMP_HEADER = "kafka.ts";
    static final String HEADER_PREFIX = "kafka.h.";
    static final String ENVELOPE_HEADER = "kafka.envelope";
    static final byte ENVELOPE_VERSION = 1;

    /** Iggy limits, from {@code core/common/src/types/message/iggy_message.rs}. */
    static final int MAX_FIELD = 255;

    static final int MAX_USER_HEADERS_SIZE = 100 * 1000;
    static final int MAX_PAYLOAD_SIZE = 64 * 1000 * 1000;

    private static final long NO_TIMESTAMP = -1;
    private static final long EPOCH_TIMESTAMP = 0;
    /** Widest origin timestamp span one Iggy send holds, in microseconds. */
    private static final long MAX_SEND_SPAN_MICROS = 0xFFFF_FFFFL;

    private static final byte[] PLACEHOLDER = {0x00};
    private static final byte[] MARKER_NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MARKER_EMPTY = "empty".getBytes(StandardCharsets.US_ASCII);
    private static final int FLAG_KEY = 0b01;
    private static final int FLAG_VALUE = 0b10;
    private static final int ENVELOPE_OVERHEAD = 13;
    private static final int ENVELOPE_HEADER_OVERHEAD = 9;

    private static final byte[] VERSION_BYTES = {MAPPING_VERSION};
    private static final HeaderValue VERSION_VALUE = HeaderValue.fromRaw(VERSION_BYTES);
    private static final byte[] HEADER_PREFIX_BYTES = HEADER_PREFIX.getBytes(StandardCharsets.UTF_8);
    private static final HeaderKey VERSION_KEY = HeaderKey.fromString(VERSION_HEADER);
    private static final HeaderKey KEY_KEY = HeaderKey.fromString(KEY_HEADER);
    private static final HeaderKey VALUE_MARKER_KEY = HeaderKey.fromString(VALUE_MARKER_HEADER);
    private static final HeaderKey TIMESTAMP_KEY = HeaderKey.fromString(TIMESTAMP_HEADER);
    private static final HeaderKey ENVELOPE_KEY = HeaderKey.fromString(ENVELOPE_HEADER);

    private RecordCodec() {}

    /** One Kafka record as the producer hands it over, after serialisation. */
    record KafkaRecord(byte[] key, byte[] value, List<Header> headers, long timestamp) {

        /** Copies the headers and refuses a repeated name, which Iggy cannot store. */
        static KafkaRecord of(byte[] key, byte[] value, Headers headers, long timestamp) {
            if (headers == null || !headers.iterator().hasNext()) {
                return new KafkaRecord(key, value, List.of(), timestamp);
            }
            List<Header> list = new ArrayList<>();
            Set<String> names = new HashSet<>();
            for (Header header : headers) {
                if (!names.add(header.key())) {
                    throw new InvalidRecordException("Record repeats header name " + header.key());
                }
                list.add(header);
            }
            return new KafkaRecord(key, value, List.copyOf(list), timestamp);
        }
    }

    /** One Iggy message read back as a Kafka record. */
    record Decoded(byte[] key, byte[] value, Headers headers, long timestamp) {}

    /** A stored message this mapping cannot read, which Kafka reports as a deserialisation error. */
    static final class CodecException extends RuntimeException {
        CodecException(String message) {
            super(message);
        }
    }

    // ---- timestamps ----

    /**
     * The origin timestamp range one Iggy send holds, starting at the batch's earliest real
     * timestamp. A timestamp outside it is clamped in, and {@value #TIMESTAMP_HEADER} keeps the
     * real one.
     */
    static final class Window {
        private final long start;

        private Window(long start) {
            this.start = start;
        }

        static Window of(List<KafkaRecord> records) {
            long start = Long.MAX_VALUE;
            for (KafkaRecord record : records) {
                if (record.timestamp() > EPOCH_TIMESTAMP) {
                    start = Math.min(start, timestampIn(record.timestamp()));
                }
            }
            return new Window(start == Long.MAX_VALUE ? 0 : start);
        }

        Stamp stamp(long millis) {
            long nativeMicros = timestampIn(millis);
            long end = start > Long.MAX_VALUE - MAX_SEND_SPAN_MICROS ? Long.MAX_VALUE : start + MAX_SEND_SPAN_MICROS;
            long origin = Math.max(start, Math.min(end, nativeMicros));
            // A zero origin reads back as "no timestamp", so the epoch needs the header too.
            Long header = origin != nativeMicros || millis == EPOCH_TIMESTAMP ? millis : null;
            return new Stamp(origin, header);
        }
    }

    record Stamp(long origin, Long header) {}

    /** A Kafka timestamp in Iggy's microseconds; throws for one that does not fit. */
    static long timestampIn(long millis) {
        if (millis == NO_TIMESTAMP) {
            return 0;
        }
        if (millis < 0 || millis > Long.MAX_VALUE / 1000) {
            throw new InvalidTimestampException(
                    "Record timestamp " + millis + " ms does not fit Iggy's microsecond field");
        }
        return millis * 1000;
    }

    // ---- Kafka to Iggy ----

    static Message toMessage(KafkaRecord record, Window window) {
        Stamp stamp = window.stamp(record.timestamp());
        List<HeaderKey> keys = storedKeys(record);
        if (keys == null || needsEnvelope(record)) {
            return envelopeMessage(record, stamp);
        }
        Map<HeaderKey, HeaderValue> headers = gatewayHeaders(stamp.header());
        byte[] payload = record.value();
        if (payload == null || payload.length == 0) {
            headers.put(VALUE_MARKER_KEY, HeaderValue.fromRaw(payload == null ? MARKER_NULL : MARKER_EMPTY));
            payload = PLACEHOLDER;
        }
        if (record.key() != null) {
            headers.put(KEY_KEY, HeaderValue.fromRaw(record.key()));
        }
        for (int i = 0; i < keys.size(); i++) {
            headers.put(keys.get(i), HeaderValue.fromRaw(record.headers().get(i).value()));
        }
        // The only limit left is the budget over all headers together; the envelope carries the rest.
        if (headersSize(headers) > MAX_USER_HEADERS_SIZE) {
            return envelopeMessage(record, stamp);
        }
        checkPayload(payload.length);
        return message(payload, headers, stamp.origin());
    }

    /** The stored key of each header, in order, or null when a name is too long for an Iggy key. */
    private static List<HeaderKey> storedKeys(KafkaRecord record) {
        List<HeaderKey> keys = new ArrayList<>(record.headers().size());
        for (Header header : record.headers()) {
            byte[] name = (HEADER_PREFIX + header.key()).getBytes(StandardCharsets.UTF_8);
            if (name.length > MAX_FIELD) {
                return null;
            }
            keys.add(new HeaderKey(HeaderKind.String, name));
        }
        return keys;
    }

    /** Whether a key or header value falls outside what an Iggy header holds; names are checked by storedKeys. */
    private static boolean needsEnvelope(KafkaRecord record) {
        byte[] key = record.key();
        if (key != null && (key.length == 0 || key.length > MAX_FIELD)) {
            return true;
        }
        for (Header header : record.headers()) {
            byte[] value = header.value();
            if (value == null || value.length == 0 || value.length > MAX_FIELD) {
                return true;
            }
        }
        return false;
    }

    private static Map<HeaderKey, HeaderValue> gatewayHeaders(Long timestamp) {
        Map<HeaderKey, HeaderValue> headers = new LinkedHashMap<>();
        headers.put(VERSION_KEY, VERSION_VALUE);
        if (timestamp != null) {
            headers.put(TIMESTAMP_KEY, HeaderValue.fromInt64(timestamp));
        }
        return headers;
    }

    private static Message envelopeMessage(KafkaRecord record, Stamp stamp) {
        Map<HeaderKey, HeaderValue> headers = gatewayHeaders(stamp.header());
        headers.put(ENVELOPE_KEY, HeaderValue.fromRaw(new byte[] {ENVELOPE_VERSION}));
        return message(encodeEnvelope(record), headers, stamp.origin());
    }

    /** Same size rule as the Java SDK's own header encoder. */
    private static long headersSize(Map<HeaderKey, HeaderValue> headers) {
        long size = 0;
        for (Map.Entry<HeaderKey, HeaderValue> entry : headers.entrySet()) {
            size += 1L
                    + 4L
                    + entry.getKey().value().length
                    + 1L
                    + 4L
                    + entry.getValue().value().length;
        }
        return size;
    }

    private static void checkPayload(long size) {
        if (size > MAX_PAYLOAD_SIZE) {
            throw new RecordTooLargeException(
                    "Record is " + size + " bytes stored, over Iggy's " + MAX_PAYLOAD_SIZE + " byte limit");
        }
    }

    private static Message message(byte[] payload, Map<HeaderKey, HeaderValue> headers, long origin) {
        MessageHeader header = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.valueOf(origin),
                headersSize(headers),
                (long) payload.length,
                BigInteger.ZERO);
        return new Message(header, payload, headers);
    }

    /** 13 bytes of fixed overhead plus 9 per header, little-endian throughout. */
    private static byte[] encodeEnvelope(KafkaRecord record) {
        long size = ENVELOPE_OVERHEAD + length(record.key()) + length(record.value());
        List<byte[]> names = new ArrayList<>();
        for (Header header : record.headers()) {
            byte[] name = header.key().getBytes(StandardCharsets.UTF_8);
            names.add(name);
            size += ENVELOPE_HEADER_OVERHEAD + name.length + length(header.value());
        }
        checkPayload(size);
        ByteBuffer buf = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        int flags = (record.key() != null ? FLAG_KEY : 0) | (record.value() != null ? FLAG_VALUE : 0);
        buf.put((byte) flags);
        putField(buf, record.key());
        putField(buf, record.value());
        buf.putInt(record.headers().size());
        for (int i = 0; i < names.size(); i++) {
            byte[] value = record.headers().get(i).value();
            putField(buf, names.get(i));
            buf.put((byte) (value != null ? 1 : 0));
            putField(buf, value);
        }
        return buf.array();
    }

    private static int length(byte[] field) {
        return field == null ? 0 : field.length;
    }

    private static void putField(ByteBuffer buf, byte[] field) {
        buf.putInt(length(field));
        if (field != null) {
            buf.put(field);
        }
    }

    // ---- Iggy to Kafka ----

    /**
     * Reads one stored message. A message without {@value #VERSION_HEADER} was written by a native
     * Iggy client: it gets a null key, its payload as the value, and every header under its own name.
     */
    static Decoded fromMessage(Message message) {
        Map<HeaderKey, HeaderValue> stored = storedHeaders(message);
        HeaderValue version = stored.get(VERSION_KEY);
        if (version == null) {
            return foreign(message, stored);
        }
        if (!Arrays.equals(version.value(), VERSION_BYTES)) {
            int v = version.value().length == 0 ? 0 : version.value()[0] & 0xFF;
            throw new CodecException("Stored mapping version " + v + " is not " + MAPPING_VERSION);
        }

        HeaderValue envelope = stored.get(ENVELOPE_KEY);
        Decoded fields =
                envelope != null ? decodeEnvelope(envelope.value(), message.payload()) : gatewayFields(message, stored);

        long timestamp;
        HeaderValue ts = stored.get(TIMESTAMP_KEY);
        if (ts == null) {
            timestamp = timestampOut(message);
        } else {
            if (ts.kind() != HeaderKind.Int64) {
                throw new CodecException("Timestamp header is not a Kafka timestamp");
            }
            long millis = ts.asInt64();
            if (millis == NO_TIMESTAMP) {
                timestamp = serverTimestamp(message);
            } else if (millis >= EPOCH_TIMESTAMP) {
                timestamp = millis;
            } else {
                throw new CodecException("Timestamp header " + millis + " is not a Kafka timestamp");
            }
        }
        return new Decoded(fields.key(), fields.value(), fields.headers(), timestamp);
    }

    /** Tells an unreadable header block from an absent one, so an envelope is never served raw. */
    private static Map<HeaderKey, HeaderValue> storedHeaders(Message message) {
        Map<HeaderKey, HeaderValue> stored = message.userHeaders();
        if (stored == null || stored.isEmpty()) {
            Long length = message.header().userHeadersLength();
            if (length != null && length > 0) {
                throw new CodecException(length + " bytes of stored user headers did not parse");
            }
            return Map.of();
        }
        return stored;
    }

    private static Decoded gatewayFields(Message message, Map<HeaderKey, HeaderValue> stored) {
        HeaderValue key = stored.get(KEY_KEY);
        byte[] value = message.payload();
        HeaderValue marker = stored.get(VALUE_MARKER_KEY);
        if (marker != null) {
            if (Arrays.equals(marker.value(), MARKER_NULL)) {
                value = null;
            } else if (Arrays.equals(marker.value(), MARKER_EMPTY)) {
                value = new byte[0];
            } else {
                throw new CodecException("Value marker is neither null nor empty");
            }
        }
        RecordHeaders headers = new RecordHeaders();
        Set<String> seen = new HashSet<>();
        for (Map.Entry<HeaderKey, HeaderValue> entry : sorted(stored)) {
            byte[] keyBytes = entry.getKey().value();
            if (!startsWith(keyBytes, HEADER_PREFIX_BYTES)) {
                continue; // Compared as bytes, so only our own keys are decoded.
            }
            String name = utf8(Arrays.copyOfRange(keyBytes, HEADER_PREFIX_BYTES.length, keyBytes.length));
            if (name == null) {
                continue;
            }
            // One key kind is written here, so two keys with one name mean the message is not ours.
            if (!seen.add(name)) {
                throw new CodecException("Two stored header keys both name " + name);
            }
            headers.add(name, entry.getValue().value());
        }
        return new Decoded(key == null ? null : key.value(), value, headers, 0);
    }

    /** A native Iggy message. Same bytes under two key kinds collapse to one name; the later wins. */
    private static Decoded foreign(Message message, Map<HeaderKey, HeaderValue> stored) {
        Map<String, byte[]> byName = new LinkedHashMap<>();
        for (Map.Entry<HeaderKey, HeaderValue> entry : sorted(stored)) {
            String name = utf8(entry.getKey().value());
            if (name != null) {
                byName.put(name, entry.getValue().value());
            }
        }
        RecordHeaders headers = new RecordHeaders();
        byName.forEach(headers::add);
        return new Decoded(null, message.payload(), headers, timestampOut(message));
    }

    private static Decoded decodeEnvelope(byte[] version, byte[] payload) {
        if (version.length == 0 || version[0] != ENVELOPE_VERSION) {
            int v = version.length == 0 ? 0 : version[0] & 0xFF;
            throw new CodecException("Envelope format version " + v + " is not " + ENVELOPE_VERSION);
        }
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int flags = take(buf, 1)[0];
        byte[] key = takeField(buf);
        byte[] value = takeField(buf);
        long count = takeLength(buf);
        // Every header costs at least nine bytes, so a count the payload cannot hold is refused up front.
        if (buf.remaining() < count * ENVELOPE_HEADER_OVERHEAD) {
            throw new CodecException("Envelope is truncated");
        }
        RecordHeaders headers = new RecordHeaders();
        for (long i = 0; i < count; i++) {
            String name = utf8(takeField(buf));
            if (name == null) {
                throw new CodecException("Envelope header name is not UTF-8");
            }
            boolean present = take(buf, 1)[0] != 0;
            byte[] headerValue = takeField(buf);
            headers.add(name, present ? headerValue : null);
        }
        if (buf.hasRemaining()) {
            throw new CodecException("Envelope has " + buf.remaining() + " bytes left after its last header");
        }
        return new Decoded((flags & FLAG_KEY) != 0 ? key : null, (flags & FLAG_VALUE) != 0 ? value : null, headers, 0);
    }

    private static byte[] take(ByteBuffer buf, int needed) {
        if (needed < 0 || buf.remaining() < needed) {
            throw new CodecException("Envelope is truncated");
        }
        byte[] out = new byte[needed];
        buf.get(out);
        return out;
    }

    private static long takeLength(ByteBuffer buf) {
        if (buf.remaining() < 4) {
            throw new CodecException("Envelope is truncated");
        }
        return Integer.toUnsignedLong(buf.getInt());
    }

    private static byte[] takeField(ByteBuffer buf) {
        long len = takeLength(buf);
        if (len > buf.remaining()) {
            throw new CodecException("Envelope is truncated");
        }
        return take(buf, (int) len);
    }

    /** Zero means the producer sent no timestamp, so the server-assigned one stands in. */
    private static long timestampOut(Message message) {
        BigInteger origin = message.header().originTimestamp();
        if (origin == null || origin.signum() == 0) {
            return serverTimestamp(message);
        }
        return origin.longValue() / 1000; // Exact for any timestamp below 2^63 microseconds.
    }

    private static long serverTimestamp(Message message) {
        BigInteger micros = message.header().timestamp();
        return micros == null ? NO_TIMESTAMP : micros.longValue() / 1000;
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        return bytes.length >= prefix.length && Arrays.equals(bytes, 0, prefix.length, prefix, 0, prefix.length);
    }

    /** Header keys in the server's order: kind first, then bytes, as the gateway emits them. */
    private static List<Map.Entry<HeaderKey, HeaderValue>> sorted(Map<HeaderKey, HeaderValue> stored) {
        List<Map.Entry<HeaderKey, HeaderValue>> entries = new ArrayList<>(stored.entrySet());
        entries.sort(Comparator.<Map.Entry<HeaderKey, HeaderValue>>comparingInt(
                        e -> e.getKey().kind().asCode())
                .thenComparing((a, b) ->
                        Arrays.compareUnsigned(a.getKey().value(), b.getKey().value())));
        return entries;
    }

    private static final ThreadLocal<CharsetDecoder> UTF8 = ThreadLocal.withInitial(() -> StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT));

    private static String utf8(byte[] bytes) {
        try {
            return UTF8.get().reset().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /** The record's own timestamp when it decodes, else Iggy's append time, for offset searches by time. */
    static long searchTimestamp(Message m) {
        try {
            return fromMessage(m).timestamp();
        } catch (RuntimeException e) {
            return m.header().timestamp().divide(BigInteger.valueOf(1000)).longValue();
        }
    }
}
