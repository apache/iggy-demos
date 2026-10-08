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

import java.util.Arrays;
import java.util.Random;

/**
 * End-to-end latencies for the current second, sampled to a fixed size. Striped by thread, so
 * the consumer threads do not queue on one monitor for every record they read.
 */
final class LatencySamples {
    private static final int STRIPES = 16;
    private static final int CAPACITY = 20_000 / STRIPES;
    private final Stripe[] stripes = new Stripe[STRIPES];

    LatencySamples() {
        for (int i = 0; i < STRIPES; i++) {
            stripes[i] = new Stripe();
        }
    }

    void add(long nanos) {
        stripes[(int) (Thread.currentThread().getId() & (STRIPES - 1))].add(nanos);
    }

    /**
     * {p50, p99} in milliseconds for the samples since the last call, or null if there were none.
     * Each stripe holds a uniform sample of one thread's reads, so a kept sample stands for
     * {@code seen / kept} reads of its stripe. The percentiles weigh samples that way, so a
     * busy thread counts for as many reads as it made, not just as many as its stripe kept.
     */
    double[] drainPercentiles() {
        long[] all = new long[STRIPES * CAPACITY];
        int[] starts = new int[STRIPES];
        int[] ends = new int[STRIPES];
        double[] weights = new double[STRIPES];
        double total = 0;
        int count = 0;
        for (int i = 0; i < STRIPES; i++) {
            starts[i] = count;
            long seen = stripes[i].drainInto(all, count);
            ends[i] = count + (int) Math.min(seen, CAPACITY);
            if (ends[i] > count) {
                weights[i] = (double) seen / (ends[i] - count);
                total += seen;
                Arrays.sort(all, count, ends[i]);
            }
            count = ends[i];
        }
        if (count == 0) {
            return null;
        }
        // Walk the sorted stripes in merged order, summing weights, until each target is passed.
        int[] pos = starts.clone();
        double covered = 0;
        double p50 = Double.NaN;
        long last = 0;
        while (true) {
            int next = -1;
            for (int i = 0; i < STRIPES; i++) {
                if (pos[i] < ends[i] && (next < 0 || all[pos[i]] < all[pos[next]])) {
                    next = i;
                }
            }
            if (next < 0) {
                break;
            }
            last = all[pos[next]++];
            covered += weights[next];
            if (Double.isNaN(p50) && covered > total * 0.50) {
                p50 = last / 1e6;
            }
            if (covered > total * 0.99) {
                break;
            }
        }
        return new double[] {Double.isNaN(p50) ? last / 1e6 : p50, last / 1e6};
    }

    /** One thread's reservoir sample of the second's latencies. */
    private static final class Stripe {
        private final long[] samples = new long[CAPACITY];
        private int count;
        private long seen;
        private final Random random = new Random();

        synchronized void add(long nanos) {
            seen++;
            if (count < CAPACITY) {
                samples[count++] = nanos;
            } else {
                long slot = (long) (random.nextDouble() * seen);
                if (slot < CAPACITY) {
                    samples[(int) slot] = nanos;
                }
            }
        }

        /**
         * Copies the samples to {@code into} from {@code at}, clears them, and returns how many
         * latencies the stripe saw; the samples copied are the lesser of that and its capacity.
         */
        synchronized long drainInto(long[] into, int at) {
            System.arraycopy(samples, 0, into, at, count);
            long result = seen;
            count = 0;
            seen = 0;
            return result;
        }
    }
}
