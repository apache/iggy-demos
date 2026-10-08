// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! End-to-end latencies for the current second, sampled to a fixed size. Striped by consumer, so
//! the consumer tasks do not queue on one lock for every message they read.

use rand::Rng;
use std::sync::Mutex;

const STRIPES: usize = 16;
const CAPACITY: usize = 20_000 / STRIPES;

pub struct LatencySamples {
    stripes: Vec<Mutex<Stripe>>,
}

/// One consumer's reservoir sample of the second's latencies.
#[derive(Default)]
struct Stripe {
    samples: Vec<u64>,
    seen: u64,
}

impl LatencySamples {
    pub fn new() -> Self {
        LatencySamples {
            stripes: (0..STRIPES)
                .map(|_| Mutex::new(Stripe::default()))
                .collect(),
        }
    }

    pub fn add(&self, stripe: usize, nanos: u64) {
        let mut s = self.stripes[stripe & (STRIPES - 1)].lock().unwrap();
        s.seen += 1;
        if s.samples.len() < CAPACITY {
            s.samples.push(nanos);
        } else {
            let slot = (rand::thread_rng().r#gen::<f64>() * s.seen as f64) as usize;
            if slot < CAPACITY {
                s.samples[slot] = nanos;
            }
        }
    }

    /// (p50, p99) in milliseconds for the samples since the last call, or None if there were
    /// none. Each stripe holds a uniform sample of one consumer's reads, so a kept sample stands
    /// for `seen / kept` reads of its stripe. The percentiles weigh samples that way, so a busy
    /// consumer counts for as many reads as it made, not just as many as its stripe kept.
    pub fn drain_percentiles(&self) -> Option<(f64, f64)> {
        let mut weighted: Vec<(u64, f64)> = Vec::new();
        let mut total = 0.0;
        for stripe in &self.stripes {
            let (samples, seen) = {
                let mut s = stripe.lock().unwrap();
                let seen = s.seen;
                s.seen = 0;
                (std::mem::take(&mut s.samples), seen)
            };
            if samples.is_empty() {
                continue;
            }
            let weight = seen as f64 / samples.len() as f64;
            total += seen as f64;
            weighted.extend(samples.into_iter().map(|v| (v, weight)));
        }
        if weighted.is_empty() {
            return None;
        }
        // Walk the samples in order, summing weights, until each target is passed.
        weighted.sort_unstable_by_key(|&(v, _)| v);
        let mut covered = 0.0;
        let mut p50 = None;
        let mut last = 0;
        for (v, w) in weighted {
            last = v;
            covered += w;
            if p50.is_none() && covered > total * 0.50 {
                p50 = Some(v as f64 / 1e6);
            }
            if covered > total * 0.99 {
                break;
            }
        }
        Some((p50.unwrap_or(last as f64 / 1e6), last as f64 / 1e6))
    }
}
