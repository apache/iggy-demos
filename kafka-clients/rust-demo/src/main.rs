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

//! A local web page that drives producers and consumers written directly against the Iggy Rust SDK
//! and shows what they measure, once a second. It does what `IggySdkDemoServer` does with the
//! Iggy Java SDK and serves the same page.
//!
//! The workload is a microservices event bus. Each stream is a domain and each of its topics an
//! event type. Each service is a consumer group with a home stream: it reads a few topics from
//! its home stream and a few from other streams, and Iggy splits each topic's partitions among
//! the service's consumers.
//!
//! Run with `cargo run --release`, then open http://localhost:8081 or its tab on the demos page.
//! Settings: `--bootstrap` (default localhost:18090), `--port` (default 8081), `--max-topic-mib`
//! (default 1024), and the Iggy password in `IGGY_PASSWORD` (default iggy).

mod latency;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::Stream;
use iggy::prelude::*;
use iggy_binary_protocol::codes::SYNC_CONSUMER_GROUP_CODE;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
use iggy_common::BinaryTransport;
use iggy_common::locking::IggyRwLockFn;
use latency::LatencySamples;
use rand::Rng;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

const PAGE: &str = include_str!("../../src/demo/resources/demo/iggy.html");

/// Every message carries the time it was made, in nanoseconds since this instant, so the
/// consumers, in the same process, measure end-to-end latency.
static EPOCH: OnceLock<Instant> = OnceLock::new();

fn now_nanos() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

struct App {
    bootstrap: String,
    password: String,
    max_topic_mib: u64,
    run: tokio::sync::Mutex<Option<Arc<Run>>>,
    events: broadcast::Sender<String>,
    /// Stopped runs still deleting their streams, so Ctrl-C can wait for them.
    cleanups: Mutex<Vec<JoinHandle<()>>>,
}

#[tokio::main]
async fn main() {
    EPOCH.get_or_init(Instant::now);
    let mut port = 8081u16;
    let mut bootstrap = "localhost:18090".to_string();
    let mut max_topic_mib = 1024u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--port" => port = value.parse().expect("--port must be a number"),
            "--bootstrap" => bootstrap = value,
            "--max-topic-mib" => {
                max_topic_mib = value.parse().expect("--max-topic-mib must be a number")
            }
            _ => panic!("Unknown option {arg}; use --port, --bootstrap or --max-topic-mib"),
        }
    }
    let password = std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| "iggy".to_string());
    let (events, _) = broadcast::channel(16);
    let app = Arc::new(App {
        bootstrap: bootstrap.clone(),
        password,
        max_topic_mib,
        run: tokio::sync::Mutex::new(None),
        events,
        cleanups: Mutex::new(Vec::new()),
    });

    let ticker = app.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        loop {
            interval.tick().await;
            let current = ticker.run.lock().await.clone();
            let json = match current {
                Some(run) => run.snapshot(),
                None => "{\"running\":false}".to_string(),
            };
            let _ = ticker.events.send(json);
        }
    });

    let router = Router::new()
        .route("/", get(page))
        .route("/start", post(start_run))
        .route("/update", post(update_run))
        .route("/stop", post(stop_run))
        .route("/events", get(event_stream))
        .fallback(|| async { (StatusCode::NOT_FOUND, "Not found") })
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("could not bind the page port");
    println!("Iggy Rust SDK demo running at http://localhost:{port} against Iggy at {bootstrap}");
    tokio::select! {
        result = axum::serve(listener, router) => result.expect("HTTP server failed"),
        _ = tokio::signal::ctrl_c() => {}
    }
    // Ctrl-C ends the run too, and waits while stopped runs delete their streams.
    stop_current(&app).await;
    let cleanups = std::mem::take(&mut *app.cleanups.lock().unwrap());
    let _ =
        tokio::time::timeout(Duration::from_secs(15), futures::future::join_all(cleanups)).await;
}

// ---- HTTP ----

async fn page() -> Response {
    let html = PAGE.replace("on the Iggy Java SDK", "on the Iggy Rust SDK");
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

fn text(status: StatusCode, body: impl Into<String>) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], body.into()).into_response()
}

/// Everything the page can set. Partitions only take effect when a run starts.
#[derive(Clone, Copy, Debug)]
struct Settings {
    size: i64,
    rate: i64,
    producers: i64,
    streams: i64,
    topics_per_stream: i64,
    partitions: i64,
    services: i64,
    topics_per_service: i64,
    consumers: i64,
    poll_interval: i64,
    skew: i64,
    linger: i64,
    batch: i64,
}

/// Most topics in a run, across all streams.
const MAX_TOPICS: i64 = 1000;

impl Settings {
    /// A modest event bus: 5 domains of 6 event types, 10 services each reading 4 of them, 512
    /// byte events at 2,500 a second in all. Consumers are per service. Skew is in tenths of the
    /// Zipf exponent: 0 sends evenly, 10 is the classic Zipf curve. Linger is how long, in
    /// milliseconds, a producer waits to fill a batch for a topic, and batch the most messages it
    /// puts in one request; Iggy's Rust producer waits 5 ms. Poll interval is how long, in
    /// milliseconds, a consumer waits before polling a topic again after finding nothing new
    /// there; Iggy's Rust consumer polls every 5 ms.
    const DEFAULTS: Settings = Settings {
        size: 512,
        rate: 2500,
        producers: 1,
        streams: 5,
        topics_per_stream: 6,
        partitions: 4,
        services: 10,
        topics_per_service: 4,
        consumers: 1,
        poll_interval: 5,
        skew: 10,
        linger: 5,
        batch: 1000,
    };

    fn parse(body: &str, defaults: Settings) -> Result<Settings, ()> {
        let form: HashMap<String, String> = form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        let number = |key: &str, fallback: i64| -> Result<i64, ()> {
            match form.get(key) {
                None => Ok(fallback),
                Some(value) => value.parse::<i32>().map(i64::from).map_err(|_| ()),
            }
        };
        Ok(Settings {
            size: number("size", defaults.size)?,
            rate: number("rate", defaults.rate)?,
            producers: number("producers", defaults.producers)?,
            streams: number("streams", defaults.streams)?,
            topics_per_stream: number("topicsPerStream", defaults.topics_per_stream)?,
            partitions: number("partitions", defaults.partitions)?,
            services: number("services", defaults.services)?,
            topics_per_service: number("topicsPerService", defaults.topics_per_service)?,
            consumers: number("consumers", defaults.consumers)?,
            poll_interval: number("pollInterval", defaults.poll_interval)?,
            skew: number("skew", defaults.skew)?,
            linger: number("linger", defaults.linger)?,
            batch: number("batch", defaults.batch)?,
        })
    }

    fn problem(&self) -> Option<String> {
        if self.size < 8 || self.size > 1_000_000 {
            return Some("Message size must be 8 to 1000000 bytes".into());
        }
        if self.rate < 0 {
            return Some("Rate must be 0 (unlimited) or more".into());
        }
        if !(1..=16).contains(&self.producers) || !(1..=16).contains(&self.consumers) {
            return Some("Producers 1 to 16, consumers per service 1 to 16".into());
        }
        if self.streams < 1
            || self.topics_per_stream < 1
            || self.streams * self.topics_per_stream > MAX_TOPICS
        {
            return Some(format!(
                "At least 1 stream and 1 topic per stream, {MAX_TOPICS} topics in all"
            ));
        }
        if !(1..=50).contains(&self.services) || !(1..=50).contains(&self.topics_per_service) {
            return Some("Services 1 to 50, topics per service 1 to 50".into());
        }
        if !(1..=64).contains(&self.partitions) {
            return Some("Partitions 1 to 64".into());
        }
        if !(0..=30).contains(&self.skew) {
            return Some("Skew 0 to 30 (tenths of the Zipf exponent)".into());
        }
        if !(0..=1000).contains(&self.poll_interval) {
            return Some("Poll interval 0 to 1000 ms".into());
        }
        if !(0..=1000).contains(&self.linger) || !(1..=100_000).contains(&self.batch) {
            return Some("Batch wait 0 to 1000 ms, batch size 1 to 100000 messages".into());
        }
        None
    }
}

async fn start_run(State(app): State<Arc<App>>, body: String) -> Response {
    let Ok(settings) = Settings::parse(&body, Settings::DEFAULTS) else {
        return text(
            StatusCode::BAD_REQUEST,
            "All settings must be whole numbers",
        );
    };
    if let Some(problem) = settings.problem() {
        return text(StatusCode::BAD_REQUEST, problem);
    }
    stop_current(&app).await;
    match Run::start(&app, settings).await {
        Ok(run) => {
            let name = run.name.clone();
            *app.run.lock().await = Some(run);
            text(StatusCode::OK, format!("started {name}"))
        }
        Err(e) => text(StatusCode::BAD_GATEWAY, format!("Could not start: {e}")),
    }
}

/// Applies new settings to the run in progress. Everything but partitions changes live.
async fn update_run(State(app): State<Arc<App>>, body: String) -> Response {
    let Some(current) = app.run.lock().await.clone() else {
        return text(StatusCode::CONFLICT, "Not running");
    };
    let Ok(settings) = Settings::parse(&body, current.settings()) else {
        return text(
            StatusCode::BAD_REQUEST,
            "All settings must be whole numbers",
        );
    };
    if let Some(problem) = settings.problem() {
        return text(StatusCode::BAD_REQUEST, problem);
    }
    match current.apply(settings).await {
        Ok(()) => text(StatusCode::OK, "updated"),
        Err(e) => text(StatusCode::BAD_GATEWAY, format!("Could not apply: {e}")),
    }
}

async fn stop_run(State(app): State<Arc<App>>) -> Response {
    stop_current(&app).await;
    text(StatusCode::OK, "stopped")
}

/// Stops the run in progress, if any, and keeps the task that is cleaning it up.
async fn stop_current(app: &App) {
    let current = app.run.lock().await.take();
    if let Some(run) = current {
        let cleanup = run.close();
        let mut cleanups = app.cleanups.lock().unwrap();
        cleanups.retain(|c| !c.is_finished());
        cleanups.push(cleanup);
    }
}

/// Server-sent events: one JSON snapshot per second.
async fn event_stream(
    State(app): State<Arc<App>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(app.events.subscribe())
        .filter_map(|json| json.ok().map(|json| Ok(Event::default().data(json))));
    Sse::new(stream)
}

// ---- one run of the workload ----

/// Retention only removes whole sealed segments and keeps at least one per partition, so small
/// segments let the cap take effect and keep the floor small with many topics.
const SEGMENT_SIZE: u64 = 4 * 1024 * 1024;

/// Iggy errors that mean a partition is being handed to another member.
const PARTITION_NOT_OWNED: u32 = 5009;
/// Iggy errors that mean the topic or group is gone, as when a topic is deleted.
const GONE: [u32; 7] = [1009, 2010, 2011, 3007, 5000, 5003, 5006];

fn gone(e: &IggyError) -> bool {
    GONE.contains(&e.as_code())
}

/// Messages written to one topic, and the highest offset written per partition.
#[derive(Default)]
struct TopicStats {
    acked: AtomicU64,
    highest_written: Mutex<HashMap<u32, i64>>,
    last_acked: AtomicU64,
}

/// Chooses topics with Zipf weights: the topic ranked r gets a share proportional to
/// 1 / r^exponent, so a few topics are hot and the rest form a long tail, as in most real
/// systems. An exponent of 0 shares sends evenly.
struct TopicPicker {
    topics: Arc<Vec<String>>,
    ordered: Vec<String>,
    skew: i64,
    cumulative: Vec<f64>,
}

impl TopicPicker {
    fn pick(&self) -> &str {
        let r = rand::thread_rng().r#gen::<f64>() * self.cumulative[self.cumulative.len() - 1];
        let i = self.cumulative.partition_point(|&c| c < r);
        &self.ordered[i.min(self.ordered.len() - 1)]
    }
}

/// Producers and consumers on a set of fresh streams and topics. Changes from the page are
/// applied by one background worker, [`Run::reconcile`], which brings the streams, topics,
/// services, their topic selections and their consumers in line with the settings, one step at
/// a time.
struct Run {
    /// Names the run. Its streams are this plus -1, -2 and so on, each holding topics t1, t2 and
    /// so on, and its services' consumer groups are this plus -s1, -s2. Inside the demo a topic
    /// is known by its stream and topic numbers, such as 3.2 for topic 2 in stream 3.
    name: String,
    bootstrap: String,
    password: String,
    /// Size cap per topic, in MiB. Iggy splits it evenly across the topic's partitions.
    max_topic_mib: u64,
    partitions: u32,
    settings: RwLock<Settings>,

    /// The run's current topics, replaced as a whole when topics are added or removed.
    topics: RwLock<Arc<Vec<String>>>,
    /// Next topic number in each stream, by stream number.
    next_topic_number: Mutex<HashMap<i64, i64>>,
    /// Streams created so far, by number.
    created_streams: Mutex<BTreeSet<i64>>,
    /// Each topic's place in the popularity order, drawn at random when it is created, so the
    /// hot topics are not always the first ones.
    popularity: Mutex<HashMap<String, f64>>,
    /// Picks a topic for each send; rebuilt when the topics or the skew change.
    picker: Mutex<Option<Arc<TopicPicker>>>,
    topic_stats: RwLock<HashMap<String, Arc<TopicStats>>>,

    producers: Mutex<Vec<Arc<Producer>>>,
    services: Mutex<Vec<Arc<Service>>>,
    /// Serialises changes to the producer list from the page.
    apply_lock: tokio::sync::Mutex<()>,

    latencies: LatencySamples,
    acked: AtomicU64,
    consumed: AtomicU64,
    consumed_bytes: AtomicU64,
    errors: AtomicU64,
    /// Request times, summed, and request counts, for the average send and poll request.
    send_nanos: AtomicU64,
    sends: AtomicU64,
    /// Messages sent in those requests, for the average batch.
    sent_in_requests: AtomicU64,
    poll_nanos: AtomicU64,
    polls: AtomicU64,
    /// Messages retention deleted before a service read them: gaps in the offsets it read.
    trimmed: AtomicU64,
    last_error: Mutex<Option<String>>,
    last: Mutex<Totals>,

    worker: mpsc::UnboundedSender<()>,
    worker_task: Mutex<Option<JoinHandle<()>>>,
    closing: AtomicBool,
    /// Hands each consumer its own latency stripe.
    next_stripe: AtomicUsize,
}

/// Totals at the last snapshot, so each snapshot reports the second since.
struct Totals {
    acked: u64,
    consumed: u64,
    bytes: u64,
    trimmed: u64,
    tick: Instant,
}

impl Run {
    async fn start(app: &App, settings: Settings) -> Result<Arc<Run>, IggyError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let (worker, mut requests) = mpsc::unbounded_channel();
        let run = Arc::new(Run {
            name: format!("demo-{millis}"),
            bootstrap: app.bootstrap.clone(),
            password: app.password.clone(),
            max_topic_mib: app.max_topic_mib,
            partitions: settings.partitions as u32,
            settings: RwLock::new(settings),
            topics: RwLock::new(Arc::new(Vec::new())),
            next_topic_number: Mutex::new(HashMap::new()),
            created_streams: Mutex::new(BTreeSet::new()),
            popularity: Mutex::new(HashMap::new()),
            picker: Mutex::new(None),
            topic_stats: RwLock::new(HashMap::new()),
            producers: Mutex::new(Vec::new()),
            services: Mutex::new(Vec::new()),
            apply_lock: tokio::sync::Mutex::new(()),
            latencies: LatencySamples::new(),
            acked: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            consumed_bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            send_nanos: AtomicU64::new(0),
            sends: AtomicU64::new(0),
            sent_in_requests: AtomicU64::new(0),
            poll_nanos: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            trimmed: AtomicU64::new(0),
            last_error: Mutex::new(None),
            last: Mutex::new(Totals {
                acked: 0,
                consumed: 0,
                bytes: 0,
                trimmed: 0,
                tick: Instant::now(),
            }),
            worker,
            worker_task: Mutex::new(None),
            closing: AtomicBool::new(false),
            next_stripe: AtomicUsize::new(0),
        });
        let started: Result<(), IggyError> = async {
            run.apply_topics().await?; // topics first, so the producers have somewhere to write
            for i in 0..settings.producers {
                let producer = Producer::start(&run, i + 1).await?;
                run.producers.lock().unwrap().push(producer);
            }
            Ok(())
        }
        .await;
        if let Err(e) = started {
            run.close();
            return Err(e);
        }
        let reconciler = run.clone();
        let task = tokio::spawn(async move {
            while requests.recv().await.is_some() {
                while requests.try_recv().is_ok() {}
                reconciler.reconcile().await;
            }
        });
        *run.worker_task.lock().unwrap() = Some(task);
        let _ = run.worker.send(());
        Ok(run)
    }

    fn settings(&self) -> Settings {
        *self.settings.read().unwrap()
    }

    fn topics(&self) -> Arc<Vec<String>> {
        self.topics.read().unwrap().clone()
    }

    fn closing(&self) -> bool {
        self.closing.load(Ordering::Relaxed)
    }

    async fn apply(self: &Arc<Self>, next: Settings) -> Result<(), IggyError> {
        let _guard = self.apply_lock.lock().await;
        *self.settings.write().unwrap() = next;
        loop {
            let count = self.producers.lock().unwrap().len() as i64;
            if count < next.producers {
                let producer = Producer::start(self, count + 1).await?;
                self.producers.lock().unwrap().push(producer);
            } else if count > next.producers {
                if let Some(producer) = self.producers.lock().unwrap().pop() {
                    producer.close();
                }
            } else {
                break;
            }
        }
        let _ = self.worker.send(());
        Ok(())
    }

    fn fail(&self, message: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().unwrap() = Some(message);
    }

    fn services(&self) -> Vec<Arc<Service>> {
        self.services.lock().unwrap().clone()
    }

    fn picker(&self, current: &Arc<Vec<String>>) -> Arc<TopicPicker> {
        let skew = self.settings().skew;
        let mut slot = self.picker.lock().unwrap();
        if let Some(p) = slot.as_ref()
            && Arc::ptr_eq(&p.topics, current)
            && p.skew == skew
        {
            return p.clone();
        }
        // Order the current topics by their random popularity, then weight them 1, 1/2^s, 1/3^s...
        let popularity = self.popularity.lock().unwrap();
        let mut ordered: Vec<String> = current.as_ref().clone();
        ordered.sort_by(|a, b| {
            let pa = popularity.get(a).copied().unwrap_or(1.0);
            let pb = popularity.get(b).copied().unwrap_or(1.0);
            pa.total_cmp(&pb)
        });
        let mut sum = 0.0;
        let cumulative = (0..ordered.len())
            .map(|i| {
                sum += ((i + 1) as f64).powf(-(skew as f64) / 10.0);
                sum
            })
            .collect();
        let p = Arc::new(TopicPicker {
            topics: current.clone(),
            ordered,
            skew,
            cumulative,
        });
        *slot = Some(p.clone());
        p
    }

    // ---- reconciling with the settings ----

    async fn reconcile(self: &Arc<Self>) {
        if self.closing() {
            return;
        }
        let result: Result<(), IggyError> = async {
            self.apply_topics().await?;
            self.apply_services();
            for service in self.services() {
                service.choose_topics(self);
            }
            for service in self.services() {
                service.apply_consumers(self).await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            self.fail(e.to_string());
        }
    }

    /// Brings the streams and their topics in line with the settings. New streams and topics are
    /// created, then handed to the producers. Removed ones are first taken away from the
    /// producers, then deleted, with their data, once in-flight sends have landed: the newest
    /// topics in each stream, or whole streams when the stream count goes down. Services drop
    /// deleted topics from their selection and pick others.
    async fn apply_topics(self: &Arc<Self>) -> Result<(), IggyError> {
        let settings = self.settings();
        let current = self.topics();
        let mut target = Vec::new();
        let mut created = Vec::new();
        let admin = connect(&self.bootstrap, &self.password).await?;
        let result: Result<(), IggyError> = async {
            for stream in 1..=settings.streams {
                let existing: Vec<&String> = current
                    .iter()
                    .filter(|t| stream_number(t) == stream)
                    .collect();
                let keep = existing.len().min(settings.topics_per_stream as usize);
                target.extend(existing[..keep].iter().map(|t| t.to_string()));
                for _ in existing.len() as i64..settings.topics_per_stream {
                    if !self.created_streams.lock().unwrap().contains(&stream) {
                        admin.create_stream(&self.stream_name(stream)).await?;
                        self.created_streams.lock().unwrap().insert(stream);
                    }
                    let number = {
                        let mut next = self.next_topic_number.lock().unwrap();
                        let n = next.entry(stream).or_insert(0);
                        *n += 1;
                        *n
                    };
                    let topic = format!("{stream}.{number}");
                    self.create_topic(&admin, &topic).await?;
                    self.topic_stats
                        .write()
                        .unwrap()
                        .insert(topic.clone(), Arc::default());
                    self.popularity
                        .lock()
                        .unwrap()
                        .insert(topic.clone(), rand::thread_rng().r#gen());
                    target.push(topic.clone());
                    created.push(topic);
                }
            }
            let removed: Vec<String> = current
                .iter()
                .filter(|t| !target.contains(t))
                .cloned()
                .collect();
            if created.is_empty() && removed.is_empty() {
                return Ok(());
            }
            target.sort_by_key(|t| key(t));
            *self.topics.write().unwrap() = Arc::new(target.clone());
            if removed.is_empty() {
                return Ok(());
            }
            for service in self.services() {
                service.choose_topics(self);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            let kept_streams: HashSet<i64> = target.iter().map(|t| stream_number(t)).collect();
            for topic in &removed {
                if kept_streams.contains(&stream_number(topic)) {
                    self.delete_topic(&admin, topic).await;
                }
                self.topic_stats.write().unwrap().remove(topic);
                self.popularity.lock().unwrap().remove(topic);
            }
            let streams: Vec<i64> = self
                .created_streams
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect();
            for stream in streams {
                if !kept_streams.contains(&stream) {
                    self.delete_stream(&admin, stream).await; // its topics and groups go with it
                }
            }
            Ok(())
        }
        .await;
        let _ = admin.shutdown().await;
        result
    }

    fn apply_services(&self) {
        let wanted = self.settings().services as usize;
        let mut services = self.services.lock().unwrap();
        while services.len() < wanted && !self.closing() {
            let number = services.len() as u32 + 1;
            services.push(Arc::new(Service::new(&self.name, number)));
        }
        while services.len() > wanted {
            if let Some(service) = services.pop() {
                service.close();
            }
        }
    }

    fn stream_name(&self, stream: i64) -> String {
        format!("{}-{stream}", self.name)
    }

    fn stream_of(&self, topic: &str) -> Identifier {
        Identifier::named(&self.stream_name(stream_number(topic))).expect("valid stream name")
    }

    // ---- reporting ----

    fn snapshot(&self) -> String {
        let mut last = self.last.lock().unwrap();
        let now = Instant::now();
        let seconds = now.duration_since(last.tick).as_secs_f64();
        last.tick = now;
        let services = self.services();
        let topics = self.topics();

        let mut per_topic = Vec::new();
        {
            let stats = self.topic_stats.read().unwrap();
            for topic in topics.iter() {
                let Some(s) = stats.get(topic) else { continue };
                let a = s.acked.load(Ordering::Relaxed);
                let read_by: Vec<String> = services
                    .iter()
                    .filter(|sv| sv.selected().contains(topic))
                    .map(|sv| sv.number.to_string())
                    .collect();
                per_topic.push(format!(
                    "{{\"label\":\"{topic}\",\"produced\":{:.1},\"readBy\":[{}]}}",
                    (a - s.last_acked.load(Ordering::Relaxed)) as f64 / seconds,
                    read_by.join(",")
                ));
                s.last_acked.store(a, Ordering::Relaxed);
            }
        }

        let mut lag = 0;
        let mut per_service = Vec::new();
        for service in &services {
            let c = service.consumed.load(Ordering::Relaxed);
            let service_lag = service.lag(self);
            lag += service_lag;
            let pct = service.latencies.drain_percentiles();
            let owners: Vec<String> = service
                .consumers()
                .iter()
                .map(|m| {
                    let owned: Vec<String> = m
                        .owned
                        .read()
                        .unwrap()
                        .iter()
                        .map(|o| format!("\"{o}\""))
                        .collect();
                    format!("[{}]", owned.join(","))
                })
                .collect();
            let selected: Vec<String> = service
                .selected()
                .iter()
                .map(|t| format!("\"{t}\""))
                .collect();
            per_service.push(format!(
                "{{\"number\":{},\"home\":{},\"topics\":[{}],\"consumers\":{},\"consumed\":{:.1},\
                 \"lag\":{},\"p50\":{},\"p99\":{},\"owners\":[{}]}}",
                service.number,
                service.home.load(Ordering::Relaxed),
                selected.join(","),
                service.consumers().len(),
                (c - service.last_consumed.load(Ordering::Relaxed)) as f64 / seconds,
                service_lag,
                num(pct.map(|p| p.0)),
                num(pct.map(|p| p.1)),
                owners.join(",")
            ));
            service.last_consumed.store(c, Ordering::Relaxed);
        }

        let a = self.acked.load(Ordering::Relaxed);
        let c = self.consumed.load(Ordering::Relaxed);
        let b = self.consumed_bytes.load(Ordering::Relaxed);
        let t = self.trimmed.load(Ordering::Relaxed);
        let pct = self.latencies.drain_percentiles();
        let s = self.settings();
        // Both read the send count, so take the batch average before the request average resets it.
        let requests = self.sends.load(Ordering::Relaxed);
        let in_requests = self.sent_in_requests.swap(0, Ordering::Relaxed);
        let batch_avg = (requests > 0).then(|| in_requests as f64 / requests as f64);
        let send_avg = average_millis(&self.send_nanos, &self.sends);
        let poll_avg = average_millis(&self.poll_nanos, &self.polls);
        let last_error = match self.last_error.lock().unwrap().as_ref() {
            Some(e) => serde_json::to_string(&e.replace('\n', " ")).unwrap(),
            None => "null".to_string(),
        };
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let json = format!(
            "{{\"running\":true,\"topic\":\"{}\",\"t\":{millis},\
             \"size\":{},\"rate\":{},\"producers\":{},\"streams\":{},\"topicsPerStream\":{},\"topicCount\":{},\"partitions\":{},\
             \"services\":{},\"topicsPerService\":{},\"consumers\":{},\"pollInterval\":{},\"skew\":{},\"linger\":{},\"batch\":{},\"gc\":[],\
             \"produced\":{:.1},\"consumed\":{:.1},\"mbps\":{:.3},\"p50\":{},\"p99\":{},\
             \"errors\":{},\"lastError\":{last_error},\"totalProduced\":{a},\"totalConsumed\":{c},\
             \"lag\":{lag},\"trimmed\":{t},\"trimmedRate\":{:.1},\"maxTopicMiB\":{},\
             \"sendLatencyAvg\":{},\"fetchLatencyAvg\":{},\"batchAvg\":{},\
             \"maxTopics\":{MAX_TOPICS},\"topicStats\":[{}],\"serviceStats\":[{}]}}",
            self.name,
            s.size,
            s.rate,
            self.producers.lock().unwrap().len(),
            s.streams,
            s.topics_per_stream,
            topics.len(),
            self.partitions,
            services.len(),
            s.topics_per_service,
            s.consumers,
            s.poll_interval,
            s.skew,
            s.linger,
            s.batch,
            (a - last.acked) as f64 / seconds,
            (c - last.consumed) as f64 / seconds,
            (b - last.bytes) as f64 / seconds / 1e6,
            num(pct.map(|p| p.0)),
            num(pct.map(|p| p.1)),
            self.errors.load(Ordering::Relaxed),
            (t - last.trimmed) as f64 / seconds,
            self.max_topic_mib,
            num(send_avg),
            num(poll_avg),
            num(batch_avg),
            per_topic.join(","),
            per_service.join(",")
        );
        last.acked = a;
        last.consumed = c;
        last.bytes = b;
        last.trimmed = t;
        json
    }

    // ---- shutting down ----

    /// Stops every producer and consumer, then deletes the run's streams, with their topics,
    /// data and consumer groups, so runs do not pile up on the server's disk. This happens on a
    /// task of its own, which is returned.
    fn close(self: &Arc<Self>) -> JoinHandle<()> {
        self.closing.store(true, Ordering::Relaxed);
        if let Some(task) = self.worker_task.lock().unwrap().take() {
            task.abort();
        }
        let producers: Vec<Arc<Producer>> = std::mem::take(&mut *self.producers.lock().unwrap());
        let services: Vec<Arc<Service>> = std::mem::take(&mut *self.services.lock().unwrap());
        let members: Vec<Arc<Member>> = services.iter().flat_map(|s| s.consumers()).collect();
        producers.iter().for_each(|p| p.close());
        members.iter().for_each(|m| m.close());
        let run = self.clone();
        tokio::spawn(async move {
            // Each producer closes its connection, and each consumer leaves its groups and
            // closes its connection, when its loop ends.
            for producer in &producers {
                join(&producer.task).await;
            }
            for member in &members {
                join(&member.task).await;
            }
            match connect(&run.bootstrap, &run.password).await {
                Ok(admin) => {
                    let streams: Vec<i64> = run
                        .created_streams
                        .lock()
                        .unwrap()
                        .iter()
                        .copied()
                        .collect();
                    for stream in streams {
                        run.delete_stream(&admin, stream).await;
                    }
                    let _ = admin.shutdown().await;
                }
                Err(e) => eprintln!("Could not delete the streams of {}: {e}", run.name),
            }
        })
    }

    // ---- talking to Iggy directly ----

    /// Creates one topic with a size cap, so a long run keeps a bounded amount of data once the
    /// services have read it. Iggy keeps no data limit by default.
    async fn create_topic(&self, admin: &IggyClient, topic: &str) -> Result<(), IggyError> {
        let options = TopicCreateOptions {
            partitions_count: Some(self.partitions),
            compression_algorithm: Some(CompressionAlgorithm::None),
            message_expiry: Some(IggyExpiry::NeverExpire),
            max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(
                self.max_topic_mib * 1024 * 1024,
            ))),
            segment_size: Some(IggyByteSize::from(SEGMENT_SIZE)),
            ..Default::default()
        };
        admin
            .create_topic(
                &self.stream_of(topic),
                &format!("t{}", topic_number(topic)),
                &options,
            )
            .await?;
        Ok(())
    }

    async fn delete_topic(&self, admin: &IggyClient, topic: &str) {
        match admin
            .delete_topic(&self.stream_of(topic), &topic_of(topic))
            .await
        {
            Ok(()) => println!("Deleted topic {topic}"),
            Err(e) => eprintln!("Could not delete topic {topic}: {e}"),
        }
    }

    async fn delete_stream(&self, admin: &IggyClient, stream: i64) {
        let name = self.stream_name(stream);
        match admin
            .delete_stream(&Identifier::named(&name).expect("valid stream name"))
            .await
        {
            Ok(()) => {
                self.created_streams.lock().unwrap().remove(&stream);
                println!("Deleted stream {name}");
            }
            Err(e) => eprintln!("Could not delete stream {name}: {e}"),
        }
    }
}

// ---- producers ----

/// Most messages made in one pass of the loop.
const ROUND: i64 = 1000;
/// Most bytes of payload in one request, whatever the batch size.
const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// Messages waiting to be sent to one topic.
#[derive(Default)]
struct Batch {
    messages: Vec<IggyMessage>,
    bytes: usize,
    first_nanos: u64,
}

/// One producer with its own connection, sending at the per-producer rate. Messages are
/// collected per topic and sent as one request when the batch is full or its first message has
/// waited the batch wait, as Iggy's Rust producer does. With no wait, the messages that are due
/// are sent straight away. Latency is measured from when a message is made, so time spent
/// waiting in a batch counts.
struct Producer {
    stopping: AtomicBool,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Producer {
    async fn start(run: &Arc<Run>, _number: i64) -> Result<Arc<Producer>, IggyError> {
        let client = connect(&run.bootstrap, &run.password).await?;
        let producer = Arc::new(Producer {
            stopping: AtomicBool::new(false),
            task: Mutex::new(None),
        });
        let task = tokio::spawn(Producer::run(run.clone(), producer.clone(), client));
        *producer.task.lock().unwrap() = Some(task);
        Ok(producer)
    }

    async fn run(run: Arc<Run>, me: Arc<Producer>, client: IggyClient) {
        let mut batches: HashMap<String, Batch> = HashMap::new();
        let mut start = Instant::now();
        let mut made: i64 = 0;
        let mut paced: i64 = -1;
        while !me.stopping.load(Ordering::Relaxed) {
            let s = run.settings();
            let rate = s.rate; // per producer, so adding producers adds throughput
            if rate != paced {
                start = Instant::now();
                made = 0;
                paced = rate;
            }
            let due = if rate == 0 {
                ROUND
            } else {
                ROUND.min((start.elapsed().as_secs_f64() * rate as f64) as i64 - made)
            };
            let mut busy = false;
            if due > 0 {
                let picker = run.picker(&run.topics());
                let size = s.size as usize;
                for _ in 0..due {
                    let now = now_nanos();
                    let mut payload = vec![0u8; size];
                    payload[..8].copy_from_slice(&now.to_be_bytes());
                    let topic = picker.pick();
                    if !batches.contains_key(topic) {
                        batches.insert(topic.to_string(), Batch::default());
                    }
                    let batch = batches.get_mut(topic).expect("inserted above");
                    if batch.messages.is_empty() {
                        batch.first_nanos = now;
                    }
                    batch.messages.push(IggyMessage::from(payload));
                    batch.bytes += size;
                    if batch.messages.len() as i64 >= s.batch || batch.bytes >= MAX_BATCH_BYTES {
                        let messages = std::mem::take(&mut batch.messages);
                        batch.bytes = 0;
                        me.send(&run, &client, topic, messages).await;
                    }
                }
                made += due;
                busy = true;
            }
            let linger = s.linger as u64 * 1_000_000;
            let now = now_nanos();
            let mut wait: u64 = 200_000;
            for (topic, batch) in batches.iter_mut() {
                if batch.messages.is_empty() {
                    continue;
                }
                let waited = now.saturating_sub(batch.first_nanos);
                if waited >= linger {
                    let messages = std::mem::take(&mut batch.messages);
                    batch.bytes = 0;
                    me.send(&run, &client, topic, messages).await;
                    busy = true;
                } else {
                    wait = wait.min(linger - waited);
                }
            }
            if !busy {
                tokio::time::sleep(Duration::from_nanos(wait)).await;
            }
        }
        let _ = client.shutdown().await;
    }

    async fn send(
        &self,
        run: &Run,
        client: &IggyClient,
        topic: &str,
        mut messages: Vec<IggyMessage>,
    ) {
        let count = messages.len() as u64;
        let started = Instant::now();
        let result = client
            .send_messages(
                &run.stream_of(topic),
                &topic_of(topic),
                &Partitioning::balanced(),
                &mut messages,
            )
            .await;
        match result {
            Ok(response) => {
                run.send_nanos
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                run.sends.fetch_add(1, Ordering::Relaxed);
                run.sent_in_requests.fetch_add(count, Ordering::Relaxed);
                run.acked.fetch_add(count, Ordering::Relaxed);
                let stats = run.topic_stats.read().unwrap().get(topic).cloned();
                if let Some(stats) = stats {
                    stats.acked.fetch_add(count, Ordering::Relaxed);
                    if let Some(confirmation) = response.confirmations.first() {
                        let last = confirmation.base_offset as i64 + count as i64 - 1;
                        let mut highest = stats.highest_written.lock().unwrap();
                        let entry = highest.entry(confirmation.partition_id).or_insert(-1);
                        *entry = (*entry).max(last);
                    }
                }
            }
            Err(e) => {
                // A topic removed from the run can still be picked by a round already under way.
                let known = run.topic_stats.read().unwrap().contains_key(topic);
                if !self.stopping.load(Ordering::Relaxed) && known {
                    run.fail(e.to_string());
                }
            }
        }
    }

    fn close(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }
}

// ---- services ----

/// One service: a consumer group with a home stream, as a microservice owns a domain. It reads
/// most of its topics from its home stream and about a quarter from other streams, and each
/// message on its topics is read once by the service.
struct Service {
    number: u32,
    group: String,
    group_id: Identifier,
    consumers: Mutex<Vec<Arc<Member>>>,
    /// The topics this service reads; replaced as a whole when the selection changes.
    selected: RwLock<Arc<Vec<String>>>,
    home: AtomicU32,
    /// Bumped when the selection changes, so consumers resubscribe on their own task.
    version: AtomicU64,
    latencies: LatencySamples,
    consumed: AtomicU64,
    last_consumed: AtomicU64,
    /// Highest offset this service has read, per topic and partition.
    highest_read: Mutex<HashMap<String, HashMap<u32, i64>>>,
}

impl Service {
    fn new(run_name: &str, number: u32) -> Service {
        let group = format!("{run_name}-s{number}");
        Service {
            number,
            group_id: Identifier::named(&group).expect("valid group name"),
            group,
            consumers: Mutex::new(Vec::new()),
            selected: RwLock::new(Arc::new(Vec::new())),
            home: AtomicU32::new(0),
            version: AtomicU64::new(0),
            latencies: LatencySamples::new(),
            consumed: AtomicU64::new(0),
            last_consumed: AtomicU64::new(0),
            highest_read: Mutex::new(HashMap::new()),
        }
    }

    fn selected(&self) -> Arc<Vec<String>> {
        self.selected.read().unwrap().clone()
    }

    fn consumers(&self) -> Vec<Arc<Member>> {
        self.consumers.lock().unwrap().clone()
    }

    /// Services take home streams in turn: service 1 stream 1, service 2 stream 2, and round
    /// again when there are more services than streams. Keeps as much of the current selection
    /// as possible, so changes elsewhere do not reshuffle every service.
    fn choose_topics(&self, run: &Run) {
        let mut selected = self.selected.write().unwrap();
        let current = run.topics();
        let settings = run.settings();
        let home_stream = (self.number as i64 - 1) % settings.streams + 1;
        let wanted = (settings.topics_per_service as usize).min(current.len());
        let home_topics: Vec<&String> = current
            .iter()
            .filter(|t| stream_number(t) == home_stream)
            .collect();
        let other_topics: Vec<&String> = current
            .iter()
            .filter(|t| stream_number(t) != home_stream)
            .collect();
        let mut from_others = if wanted < 2 || other_topics.is_empty() {
            0
        } else {
            (wanted / 4).max(1)
        };
        let from_home = (wanted - from_others).min(home_topics.len());
        from_others = (wanted - from_home).min(other_topics.len());
        let mut next = pick(&selected, &home_topics, from_home);
        next.extend(pick(&selected, &other_topics, from_others));
        next.sort_by_key(|t| key(t));
        if home_stream as u32 != self.home.load(Ordering::Relaxed) || next != **selected {
            *selected = Arc::new(next);
            self.home.store(home_stream as u32, Ordering::Relaxed);
            self.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Adds consumers one at a time, each after the previous one has partitions in every topic.
    /// Iggy rebalances a group only when membership changes, and counts partitions still being
    /// handed over as moved, so consumers that join together can leave one of them with nothing.
    /// Removing consumers needs no pause.
    async fn apply_consumers(self: &Arc<Self>, run: &Arc<Run>) -> Result<(), IggyError> {
        let wanted = run.settings().consumers as usize;
        loop {
            let removed = {
                let mut consumers = self.consumers.lock().unwrap();
                if consumers.len() > wanted {
                    consumers.pop()
                } else {
                    None
                }
            };
            match removed {
                Some(member) => member.close(),
                None => break,
            }
        }
        while self.consumers.lock().unwrap().len() < wanted && !run.closing() {
            let added = Member::start(run, self).await?;
            let count = {
                let mut consumers = self.consumers.lock().unwrap();
                consumers.push(added.clone());
                consumers.len()
            };
            let topics_wanted = if count <= run.partitions as usize {
                self.selected().len()
            } else {
                0
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while added.topics_owned() < topics_wanted
                && Instant::now() < deadline
                && !run.closing()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    /// Messages on this service's topics that it has not read yet.
    fn lag(&self, run: &Run) -> i64 {
        let stats = run.topic_stats.read().unwrap();
        let highest_read = self.highest_read.lock().unwrap();
        let mut lag = 0;
        for topic in self.selected().iter() {
            let Some(s) = stats.get(topic) else { continue };
            let read = highest_read.get(topic);
            for (partition, written) in s.highest_written.lock().unwrap().iter() {
                let r = read.and_then(|r| r.get(partition)).copied().unwrap_or(-1);
                lag += (written - r).max(0);
            }
        }
        lag
    }

    /// Offsets in a partition are consecutive, so a jump past the highest offset read means
    /// retention deleted the messages in between before this service read them. Offsets at or
    /// below it are re-reads after a rebalance and are ignored.
    fn note_reads(
        &self,
        run: &Run,
        topic: &str,
        partition: u32,
        offsets: impl Iterator<Item = u64>,
    ) {
        let mut highest_read = self.highest_read.lock().unwrap();
        let highest = highest_read
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_insert(-1);
        for offset in offsets {
            let offset = offset as i64;
            if offset > *highest {
                if offset > *highest + 1 {
                    run.trimmed
                        .fetch_add((offset - *highest - 1) as u64, Ordering::Relaxed);
                }
                *highest = offset;
            }
        }
    }

    fn close(&self) {
        self.consumers().iter().for_each(|m| m.close());
    }
}

/// Up to `count` of the candidates, keeping those already selected first.
fn pick(selected: &[String], candidates: &[&String], count: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let mut kept: Vec<String> = candidates
        .iter()
        .filter(|c| selected.contains(c))
        .map(|c| c.to_string())
        .collect();
    while kept.len() > count {
        kept.remove(rng.gen_range(0..kept.len()));
    }
    let mut spare: Vec<String> = candidates
        .iter()
        .filter(|c| !kept.contains(c))
        .map(|c| c.to_string())
        .collect();
    while kept.len() < count && !spare.is_empty() {
        kept.push(spare.remove(rng.gen_range(0..spare.len())));
    }
    kept
}

/// Most messages one poll returns.
const POLL_COUNT: u32 = 5000;

/// One consumer of a service: a member of the service's group on each of its topics. Iggy
/// consumer groups are per topic, so it joins the group on each topic, then polls the topics in
/// turn with no partition given: the SDK picks the next partition this member owns, the server
/// returns the messages after the group's stored offset, and stores the new offset as it does
/// (auto-commit).
struct Member {
    stopping: AtomicBool,
    /// Owned partitions as "topic:partition", such as 3.2:1, published by the consumer task.
    owned: RwLock<Vec<String>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

/// The consumer task's own state.
struct Polling {
    run: Arc<Run>,
    service: Arc<Service>,
    me: Arc<Member>,
    client: IggyClient,
    consumer: Consumer,
    stripe: usize,
    /// Topics this consumer has joined the group on.
    joined: Vec<String>,
    subscribed_version: u64,
    next_assignment_check: Instant,
}

impl Member {
    async fn start(run: &Arc<Run>, service: &Arc<Service>) -> Result<Arc<Member>, IggyError> {
        let client = connect(&run.bootstrap, &run.password).await?;
        let me = Arc::new(Member {
            stopping: AtomicBool::new(false),
            owned: RwLock::new(Vec::new()),
            task: Mutex::new(None),
        });
        let polling = Polling {
            run: run.clone(),
            service: service.clone(),
            me: me.clone(),
            client,
            consumer: Consumer::group(service.group_id.clone()),
            stripe: run.next_stripe.fetch_add(1, Ordering::Relaxed),
            joined: Vec::new(),
            subscribed_version: u64::MAX,
            next_assignment_check: Instant::now(),
        };
        *me.task.lock().unwrap() = Some(tokio::spawn(polling.run()));
        Ok(me)
    }

    fn topics_owned(&self) -> usize {
        let owned = self.owned.read().unwrap();
        owned
            .iter()
            .map(|o| o.split(':').next().unwrap_or_default())
            .collect::<HashSet<_>>()
            .len()
    }

    fn close(&self) {
        self.stopping.store(true, Ordering::Relaxed);
    }
}

impl Polling {
    fn stopping(&self) -> bool {
        self.me.stopping.load(Ordering::Relaxed)
    }

    /// Joins the group on newly selected topics and leaves it on dropped ones.
    async fn resubscribe(&mut self) {
        self.subscribed_version = self.service.version.load(Ordering::Relaxed);
        let wanted = self.service.selected();
        let mut next = Vec::new();
        for topic in std::mem::take(&mut self.joined) {
            if wanted.contains(&topic) {
                next.push(topic);
            } else {
                // Best effort: the server drops the member when the connection closes anyway.
                let _ = self
                    .client
                    .leave_consumer_group(
                        &self.run.stream_of(&topic),
                        &topic_of(&topic),
                        &self.service.group_id,
                    )
                    .await;
            }
        }
        for topic in wanted.iter() {
            if !next.contains(topic) && self.join(topic).await {
                next.push(topic.clone());
            }
        }
        self.joined = next;
        self.next_assignment_check = Instant::now();
    }

    async fn join(&self, topic: &str) -> bool {
        let stream = self.run.stream_of(topic);
        let topic_id = topic_of(topic);
        let group = &self.service.group_id;
        let result: Result<(), IggyError> = async {
            if self
                .client
                .get_consumer_group(&stream, &topic_id, group)
                .await?
                .is_none()
                && let Err(e) = self
                    .client
                    .create_consumer_group(&stream, &topic_id, &self.service.group)
                    .await
            {
                // Another consumer of the service may have created it first.
                if self
                    .client
                    .get_consumer_group(&stream, &topic_id, group)
                    .await?
                    .is_none()
                {
                    return Err(e);
                }
            }
            self.client
                .join_consumer_group(&stream, &topic_id, group)
                .await
        }
        .await;
        match result {
            Ok(()) => true,
            Err(e) => {
                if !gone(&e) {
                    self.run.fail(e.to_string());
                }
                false
            }
        }
    }

    /// Polls each topic in turn. A topic that had nothing new is left alone for the poll
    /// interval, as Iggy's Rust consumer does, so idle topics do not keep the server busy; a
    /// topic that returned messages is polled again straight away.
    async fn run(mut self) {
        let mut idle_until: HashMap<String, Instant> = HashMap::new();
        while !self.stopping() {
            let result: Result<(), IggyError> = async {
                if self.subscribed_version != self.service.version.load(Ordering::Relaxed) {
                    self.resubscribe().await;
                    idle_until.retain(|t, _| self.joined.contains(t));
                }
                if Instant::now() >= self.next_assignment_check {
                    self.publish_assignment().await?;
                }
                let interval = Duration::from_millis(self.run.settings().poll_interval as u64);
                let now = Instant::now();
                let mut wake = now + interval.max(Duration::from_millis(1));
                let mut any = false;
                for topic in self.joined.clone() {
                    if self.stopping() {
                        break;
                    }
                    if let Some(&until) = idle_until.get(&topic)
                        && until > now
                    {
                        wake = wake.min(until);
                        continue;
                    }
                    if self.poll(&topic).await? {
                        any = true;
                        idle_until.remove(&topic);
                    } else {
                        idle_until.insert(topic, Instant::now() + interval);
                    }
                }
                if !any {
                    // Nothing new anywhere: sleep until the first topic is due again.
                    let floor = if interval.is_zero() {
                        Duration::from_millis(1)
                    } else {
                        Duration::ZERO
                    };
                    tokio::time::sleep(floor.max(wake.saturating_duration_since(Instant::now())))
                        .await;
                }
                Ok(())
            }
            .await;
            if let Err(e) = result
                && !self.stopping()
            {
                self.run.fail(e.to_string());
            }
        }
        // Leaving hands this consumer's partitions to the others straight away.
        for topic in &self.joined {
            let _ = self
                .client
                .leave_consumer_group(
                    &self.run.stream_of(topic),
                    &topic_of(topic),
                    &self.service.group_id,
                )
                .await;
        }
        let _ = self.client.shutdown().await;
    }

    async fn poll(&mut self, topic: &str) -> Result<bool, IggyError> {
        let started = Instant::now();
        let polled = match self
            .client
            .poll_messages(
                &self.run.stream_of(topic),
                &topic_of(topic),
                None,
                &self.consumer,
                &PollingStrategy::next(),
                POLL_COUNT,
                true,
            )
            .await
        {
            Ok(polled) => polled,
            Err(e) if e.as_code() == PARTITION_NOT_OWNED || gone(&e) => {
                self.next_assignment_check = Instant::now();
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        self.run
            .poll_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.run.polls.fetch_add(1, Ordering::Relaxed);
        if polled.messages.is_empty() {
            return Ok(false);
        }
        let now = now_nanos();
        let mut bytes = 0u64;
        for message in &polled.messages {
            let sent =
                u64::from_be_bytes(message.payload[..8].try_into().expect("8 byte timestamp"));
            let latency = now.saturating_sub(sent);
            self.run.latencies.add(self.stripe, latency);
            self.service.latencies.add(self.stripe, latency);
            bytes += message.payload.len() as u64;
        }
        self.service.note_reads(
            &self.run,
            topic,
            polled.partition_id,
            polled.messages.iter().map(|m| m.header.offset),
        );
        let n = polled.messages.len() as u64;
        self.run.consumed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.service.consumed.fetch_add(n, Ordering::Relaxed);
        self.run.consumed.fetch_add(n, Ordering::Relaxed);
        Ok(true)
    }

    /// Asks Iggy which partitions this member owns in each topic, for the page.
    async fn publish_assignment(&mut self) -> Result<(), IggyError> {
        self.next_assignment_check = Instant::now() + Duration::from_millis(500);
        let mut result = Vec::new();
        for topic in &self.joined {
            match self.sync_assignment(topic).await {
                Ok(mut partitions) => {
                    partitions.sort_unstable();
                    result.extend(partitions.iter().map(|p| format!("{topic}:{p}")));
                }
                Err(e) if gone(&e) => {}
                Err(e) => return Err(e),
            }
        }
        *self.me.owned.write().unwrap() = result;
        Ok(())
    }

    /// The partitions this member owns in one topic. The SDK keeps the assignment it polls by
    /// to itself, and the group details the server returns number members by position rather
    /// than by client, so this sends the SyncConsumerGroup request the SDK syncs with.
    async fn sync_assignment(&self, topic: &str) -> Result<Vec<u32>, IggyError> {
        let wire =
            |name: String| WireIdentifier::named(name).map_err(|_| IggyError::InvalidCommand);
        let request = SyncConsumerGroupRequest {
            stream_id: wire(self.run.stream_name(stream_number(topic)))?,
            topic_id: wire(format!("t{}", topic_number(topic)))?,
            group_id: wire(self.service.group.clone())?,
        };
        let transport = self.client.client();
        let transport = transport.read().await;
        let ClientWrapper::Tcp(tcp) = &*transport else {
            return Ok(Vec::new());
        };
        let response = tcp
            .send_raw_with_response(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new()); // not a member
        }
        let (assignment, _) =
            SyncConsumerGroupResponse::decode(&response).map_err(|_| IggyError::InvalidCommand)?;
        Ok(assignment.partitions)
    }
}

// ---- helpers ----

/// Orders topics by stream number, then by topic number within the stream.
fn key(topic: &str) -> (i64, i64) {
    (stream_number(topic), topic_number(topic))
}

fn stream_number(topic: &str) -> i64 {
    topic
        .split('.')
        .next()
        .and_then(|s| s.parse().ok())
        .expect("topic key is stream.topic")
}

fn topic_number(topic: &str) -> i64 {
    topic
        .split('.')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("topic key is stream.topic")
}

fn topic_of(topic: &str) -> Identifier {
    Identifier::named(&format!("t{}", topic_number(topic))).expect("valid topic name")
}

async fn connect(bootstrap: &str, password: &str) -> Result<IggyClient, IggyError> {
    let client = IggyClient::builder()
        .with_tcp()
        .with_server_address(bootstrap.to_string())
        .build()?;
    client.connect().await?;
    client.login_user("iggy", password).await?;
    Ok(client)
}

async fn join(task: &Mutex<Option<JoinHandle<()>>>) {
    let handle = task.lock().unwrap().take();
    if let Some(handle) = handle {
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }
}

/// Mean request time in milliseconds since the last call, then starts again.
fn average_millis(nanos: &AtomicU64, count: &AtomicU64) -> Option<f64> {
    let n = count.swap(0, Ordering::Relaxed);
    let total = nanos.swap(0, Ordering::Relaxed);
    (n > 0).then(|| total as f64 / 1e6 / n as f64)
}

fn num(v: Option<f64>) -> String {
    match v {
        Some(v) if v.is_finite() => format!("{v:.3}"),
        _ => "null".to_string(),
    }
}
