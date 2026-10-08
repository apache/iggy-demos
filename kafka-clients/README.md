# Iggy Kafka clients

Kafka's `Producer`, `Consumer` and `Admin` interfaces, backed by Apache Iggy. Change the constructor and the rest of your Kafka client code stays as it is:

```java
Producer<String, String> producer = new IggyKafkaProducer<>(props);   // was new KafkaProducer<>(props)
Consumer<String, String> consumer = new IggyKafkaConsumer<>(props);   // was new KafkaConsumer<>(props)
Admin admin = IggyAdminClient.create(props);                          // was AdminClient.create(props)
```

Built with Java 17 against `kafka-clients` 4.3.0 and the Iggy Java SDK 0.9.0, for an Iggy 0.9.x server. The jar also runs with `kafka-clients` 3.9 on the classpath, and keeps the 3.x methods that 4.0 removed: `poll(long)`, `committed(TopicPartition)` and `alterConfigs`. Older 3.x versions have not been tried.

The jar is not in any Maven repository. Build it with `mvn package` (see Building and testing below).

## Configuration

These Kafka settings work as they do in Kafka: `bootstrap.servers` (only the first entry is used), `key.serializer`, `value.serializer`, `key.deserializer`, `value.deserializer`, `client.id`, `linger.ms`, `buffer.memory`, `max.block.ms`, `metadata.max.age.ms`, `request.timeout.ms`, `group.id`, `enable.auto.commit`, `auto.commit.interval.ms`, `auto.offset.reset`, `max.poll.records` and `allow.auto.create.topics`.

`max.partition.fetch.bytes` (default 1 MiB) caps each fetch from a partition, as in Kafka, but approximately: Iggy counts a request in messages, so the client sizes each request from the mean payload size of the partition's last fetch. Keys and headers are not counted. The cap only matters when a consumer is catching up on a backlog. Raising it lets a consumer catch up in fewer requests, at the cost of more buffered memory per assigned partition.

Settings specific to Iggy:

| Key | Default | Meaning |
|---|---|---|
| `iggy.stream` | `kafka` | The Iggy stream that holds every Kafka topic |
| `iggy.username` / `iggy.password` | `iggy` / none | Iggy credentials. A password must be set here or through SASL/PLAIN |
| `iggy.auto.create.topics` | `true` | Create missing streams and topics |
| `iggy.default.partitions` | `1` | Partition count for topics created automatically |
| `iggy.assignment.refresh.ms` | `1000` | How often a subscribed consumer asks Iggy for its partitions |
| `iggy.poll.idle.ms` | `20` | How long `poll()` waits before trying again after a pass over its partitions found nothing. Lower values cut latency after a quiet spell at the cost of more requests |
| `iggy.fetch.max.records` | `5000` | Most records fetched per partition per request. After the first fetch from a partition, the count is cut to keep a request near `max.partition.fetch.bytes`. Fetched records are buffered per assigned partition, and `max.poll.records` caps how many each `poll()` hands back |

## Security

- `security.protocol` `SSL` or `SASL_SSL` connects with TLS.
- The truststore can be PEM (`ssl.truststore.location` or `ssl.truststore.certificates`), JKS or PKCS12. Without one, the JVM's default trust store is used.
- With `SASL_PLAINTEXT` or `SASL_SSL`, `sasl.mechanism` must be `PLAIN`. The username and password in `sasl.jaas.config` become the Iggy credentials, as in the Iggy Kafka gateway.
- `iggy.username` and `iggy.password` win over SASL when both are set.
- Client certificates (`ssl.keystore.*`) are rejected.

## Storage format

Records are stored the way the Iggy Kafka gateway stores them (`gateways/kafka/docs/BRIDGE_MAPPING.md` in apache/iggy, mapping version 1). Records written by this library can be read through the gateway, and the reverse.

- A Kafka topic is an Iggy topic of the same name in `iggy.stream`. Partitions are 0-based in both.
- The value is the Iggy payload, so native Iggy consumers can read it.
- The key goes in the `kafka.key` header. Each Kafka header goes in `kafka.h.<name>`.
- Every record carries a `kafka.v` header. A message without it was written by a native Iggy client. It reads back with a null key and its headers under their own names.
- A null or empty value is stored as one `0x00` byte plus a `kafka.value` marker header.
- Anything Iggy cannot hold as a header is packed into the payload instead and flagged with `kafka.envelope`: an empty key, a key over 255 bytes, or a header value that is null, empty or over 255 bytes. Native Iggy consumers have to decode the envelope to read these.
- The record timestamp is kept and read back as `CreateTime`.
- Keyed records go to `murmur2(key) % partitions`, the same partition Kafka would pick. The partition count is refreshed every `metadata.max.age.ms`, so partitions added on the server get used.
- Unkeyed records use Iggy's balanced partitioning: the Iggy SDK spreads each batch over the partitions in turn, with a partition count it refreshes every few seconds.
- `subscribe()` joins an Iggy consumer group named after `group.id`. Iggy assigns the partitions, and changes reach a `ConsumerRebalanceListener` during `poll()`.
- A committed offset means the next offset to read, as in Kafka. Iggy stores the last consumed offset, and the client converts between the two.

## Admin

`IggyAdminClient` extends Kafka's `AdminClient`, so it fits wherever `Admin` or `AdminClient` is used. `IggyAdminClient.create(props)` mirrors `AdminClient.create(props)`, so a call site changes only the class name.

What maps onto Iggy:

- Topics: `createTopics`, `deleteTopics`, `listTopics`, `describeTopics` (by name or id) and `createPartitions`. A topic's id is the stream's numeric id over the topic's. `createTopics` honours `retention.ms` (Iggy message expiry), `retention.bytes` (Iggy's topic size cap) and `compression.type` (`none` or `gzip`). Other topic configs and the replication factor are ignored.
- `describeConfigs` on a topic returns `retention.ms`, `retention.bytes` and `compression.type`. Other resource types fail with `InvalidRequestException`.
- `describeCluster` lists the cluster's nodes, leader first. A single node is reported at the bootstrap address.
- Consumer groups: `listConsumerGroups`, `listGroups`, `describeConsumerGroups`, `deleteConsumerGroups`, `listConsumerGroupOffsets` and `alterConsumerGroupOffsets`. Iggy groups are per topic, so a Kafka group is every Iggy group of that name across the stream. Member ids are Iggy client ids. As in Kafka, a group with active members can neither be deleted nor have its offsets changed.
- `listOffsets` for earliest, latest and a timestamp. The timestamp search uses Iggy's append time, as `offsetsForTimes()` does.

Limits:

- Calls run on the caller's thread and return completed futures. The `timeoutMs` in each options object is ignored; `request.timeout.ms` bounds each Iggy request.
- Iggy only takes group offsets from a member, so `alterConsumerGroupOffsets` joins the empty group on each topic, stores the offsets and leaves. Offset 0 cannot be stored, because Iggy has no encoding for "nothing consumed"; delete the group to start over.
- Offsets committed through `assign()`, which the consumer stores under a standalone consumer, are not visible to the admin client.
- ACLs, quotas, SCRAM credentials, delegation tokens, log directories, reassignments, leader elections, the Raft quorum, transactions, share groups, streams groups, config changes, record deletion, deleting single offsets and removing members throw `UnsupportedOperationException`.

## Metrics

`metrics()` returns a handful of metrics under Kafka's own names and groups, tagged with `client-id`. Tools that read Kafka client metrics, such as Micrometer's Kafka binders, pick them up.

| Group | Metrics |
|---|---|
| `producer-metrics` | `record-send-total`, `record-send-rate`, `record-error-total`, `record-error-rate`, `request-latency-avg`, `buffer-available-bytes` |
| `consumer-fetch-manager-metrics` | `records-consumed-total`, `records-consumed-rate`, `bytes-consumed-total`, `bytes-consumed-rate`, `fetch-total`, `fetch-rate`, `fetch-latency-avg`, and `records-lag` per assigned partition (tagged `topic` and `partition`) |
| `consumer-coordinator-metrics` | `commit-total`, `commit-rate`, `assigned-partitions` |
| `admin-client-metrics` | `request-total`, `request-rate`, `failed-request-total`, `failed-request-rate`, `request-latency-avg` |

Nothing is exported over JMX.

## Limits

- No transactions or exactly-once. `initTransactions()` and the other transaction methods throw.
- Failed sends are not retried. After a callback error the record may or may not have been stored.
- A repeated header name is rejected. Header order is not kept: headers come back sorted by name.
- Offsets are not yet stored the way the gateway stores them, so a consumer group cannot move between this library and the gateway.
- `assign()` with a `group.id` stores offsets under a standalone Iggy consumer named after the group, because Iggy only accepts group offsets from the member that owns the partition. These offsets are separate from the ones written through `subscribe()`. Iggy hashes standalone consumer names to 32 bits, so two group names can collide.
- Committing offset 0 does nothing, because Iggy cannot store "nothing consumed".
- A consumer can commit offsets for a group it has not joined only while that group has no members, as in Kafka. It joins the group for the commit and leaves again; otherwise `commitSync` throws `CommitFailedException`.
- `commitAsync` commits on the calling thread and runs its callback before it returns.
- `offsetsForTimes()` searches by Iggy append time, not by record timestamp.
- Iggy has no long poll. With nothing buffered, `poll()` sends one request per assigned partition, then waits `iggy.poll.idle.ms` (20 ms by default) if nothing came back. An idle consumer on many partitions keeps sending requests, and after a quiet spell a record can take up to that wait to arrive.
- `currentLag()` is empty until the first fetch from the partition, then reports the lag as of the latest fetch. `subscribe(SubscriptionPattern)` is unsupported.
- One connection per client. Only the first `bootstrap.servers` entry is used.

## Demos

Three demos share one local web page, with a tab for each: the Iggy Java SDK, the Iggy Rust SDK, and this library's Kafka clients. Each runs producers and consumers against an Iggy server and charts throughput and end-to-end latency while they run. A fourth tab starts a three-node Iggy cluster in Docker for them to run against.

Start an Iggy server on `localhost:18090`, or use the cluster tab, then start the Java demos:

```bash
mvn -Pdemo compile exec:java
```

For the Rust tab, start the Rust demo as well, in a second terminal:

```bash
cd rust-demo && cargo run --release
```

Open http://localhost:8080. `-Ddemo.port` changes the page port, `-Ddemo.bootstrap` the Iggy address, `-Ddemo.rustUrl` where the Rust tab finds its demo (http://localhost:8081/ by default), and `IGGY_PASSWORD` the password when it is not `iggy`.

Only one demo runs at a time. Starting a run in one tab stops the run in the others first, so no run measures another's load. A tab keeps its charts while you look at another, and a dot on the tab marks the demo that is running.

Each demo also has a main of its own, used by the benchmark script: `-Ddemo.main=org.apache.iggy.kafka.demo.DemoServer` or `-Ddemo.main=org.apache.iggy.kafka.demo.IggySdkDemoServer` serves just that demo's page at http://localhost:8080.

Iggy's segment cleaner runs once a minute by default, which lets a full-speed run overshoot the topic cap by several GB between passes. Start Iggy with `IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL=5s` for the demo.

The demo code lives in `src/demo` and only compiles under the `demo` profile. Run `mvn clean` before packaging the library, so the demo classes are not left in `target/classes`.

## Kafka clients demo

The Kafka clients tab runs producers and consumers from this library. Consumers are grouped into services. A service is one consumer group with some number of instances, reading its own random pick of topics. A checkbox turns service 1 into a catch-all that reads every topic.

### Controls

Sliders set:

- message size, rate per producer and number of producers
- the producers' batch wait (`linger.ms`) and the consumers' poll wait (`iggy.poll.idle.ms`)
- number of topics, partitions per topic and topic skew
- number of services, topics per service and instances per service

Everything but partitions can be changed mid-run. Partitions are fixed for a run, and every start uses fresh topics.

### What you will see

Each send picks a topic at random with Zipf weights, so a few topics are busy and the rest form a long tail. The skew slider goes from even to very steep. Which topics are the busy ones is reshuffled each run.

Every service that reads a topic reads each of its messages once, so the consumed rate can be higher than the produced rate.

An instance reads its partitions one at a time, so latency grows with the number of partitions an instance owns. The table shows each service's topics, instances, partitions per instance, rate, lag and latency. The latency chart has a line for each of the first four services.

The JVM reports each garbage collection pause, and the demo marks any second with a pause of 10 ms or more on the charts in red, labelled with the pause length. The tooltip and the data table list every pause, however short, so a latency spike can be matched to a pause or ruled out.

Changing the topic count creates the new topics and hands them to the producers, or takes the newest topics away from the producers and deletes them with their data. Services drop deleted topics and pick others, keeping as much of their current selection as they can, and only a service whose selection changed resubscribes. A service that picks up a topic it was not reading starts from the earliest offset, so its latency jumps until it clears the backlog.

Raising the instance count adds instances one at a time, each after the previous one holds partitions in every topic. Iggy 0.9.0 can leave a consumer with no partitions when several join while partitions are still being handed over.

### Data and cleanup

Each topic is capped at 1 GiB (`-Ddemo.maxTopicMiB`) with 4 MiB segments. Once a topic passes the cap, Iggy removes its oldest sealed segments whether or not a service has read them. A service that falls that far behind loses the messages in those segments, and the page reports them as trimmed.

A run's topics and their data are deleted when the run stops, including when the demo itself is stopped with Ctrl-C.

## Iggy SDK demo

The Iggy Java SDK tab, `IggySdkDemoServer`, has producers and consumers written directly against the Iggy Java SDK. It models a microservices event bus. Each stream is a domain and each of its topics an event type, up to 1,000 topics in all.

Each service is a consumer group with a home stream, the way a microservice owns a domain. Service 1 lives in stream 1, service 2 in stream 2, and round again when there are more services than streams. A service reads about three quarters of its topics from its home stream and the rest from other streams. Iggy splits each topic's partitions among the service's consumers.

### Controls

Sliders set:

- message size, rate per producer, number of producers and topic skew
- the producers' batch wait and batch size
- number of streams, topics per stream and partitions per topic
- number of services, topics per service, consumers per service and the consumers' poll interval

The defaults are 5 streams of 6 topics, 10 services reading 4 topics each with 1 consumer, and 512 byte events at 2,500 a second. Everything but partitions can be changed mid-run.

### How it works

Each consumer joins its service's group on every topic it reads and polls with `PollingStrategy.next()` and auto-commit, so the server picks the partition. When a poll finds nothing new on a topic, the consumer waits the poll interval before trying that topic again, 5 ms by default as in Iggy's Rust consumer.

Producers batch per topic. A batch is sent when it reaches the batch size or when its first message has waited the batch wait, 5 ms by default as in Iggy's Rust producer. Latency includes both waits.

Each send picks a topic at random with Zipf weights, as in the Kafka demo. The page lists topics busiest first as stream.topic, so 3.2 is topic 2 in stream 3.

### What you will see

Latency runs from when a message is made to when a service reads it, measured in the one JVM. The page shows p50 and p99 for the run and for each service, next to the service's home stream, topics, consumers, partitions per consumer, rate and lag.

Each message is read once by every service that reads its topic, so the consumed rate can be higher than the produced rate. A consumer reads its partitions one at a time, so latency grows with the partitions each consumer owns.

The JVM reports each garbage collection pause, and the demo marks any second with a pause of 10 ms or more on the charts in red, labelled with the pause length. The tooltip and the data table list every pause, however short, so a latency spike can be matched to a pause or ruled out.

Changing the stream or topic count creates the new topics and hands them to the producers. Removed topics are taken away from the producers first, then deleted with their data once in-flight sends have landed. Removing topics deletes the newest in each stream, and removing streams deletes whole streams. Services drop deleted topics and pick others, keeping as much of their current selection as they can, so a change in one place does not reshuffle every service.

Raising the consumer count adds consumers one at a time, each after the previous one holds partitions in every topic, for the same Iggy 0.9.0 reason as in the Kafka demo. Removing consumers needs no pause.

### Data and cleanup

Each topic is capped at 1 GiB (`-Ddemo.maxTopicMiB`) with 4 MiB segments. Retention removes whole sealed segments and keeps at least one per partition. A message that retention deleted before a service read it shows up as a gap in the offsets that service reads, and the page counts these as trimmed.

A run's streams, with their topics, data and consumer groups, are deleted when the run stops, including when the demo is stopped with Ctrl-C.

## Iggy cluster tab

The Iggy cluster tab starts three Iggy nodes in Docker, kept consistent by Viewstamped Replication, and stops and starts each node on its own. Stop a node and the other two carry on; stop the leader and a follower takes over; start a node again and it catches up. The tab shows each container's state from Docker and the cluster's own view of the nodes, roles included, read from the first node whose HTTP API answers. Each node card also shows its last log lines.

Node 1 serves TCP on `localhost:18090`, the demos' default bootstrap address, so the other tabs run against the cluster once it is up. Stop any standalone Iggy server on that port first, or node 1 cannot publish it.

| Node | Container | TCP | HTTP |
| --- | --- | --- | --- |
| iggy-node-1 | iggy-demo-node-1 | 18090 | 13000 |
| iggy-node-2 | iggy-demo-node-2 | 18091 | 13001 |
| iggy-node-3 | iggy-demo-node-3 | 18092 | 13002 |

The containers run `apache/iggy:0.9.0` (`-Ddemo.iggyImage`) on a Docker network of their own, `iggy-demo-cluster`, with the Docker command found on the path (`-Ddemo.docker`). The first start pulls the image. `IGGY_PASSWORD` is the root password on the cluster's first boot. Stopping the cluster, or quitting the demos, stops the containers and keeps their data for the next start; tick Start from empty data to remove them first.

Two settings go beyond the Deploy a Cluster page: the containers run with `--security-opt seccomp=unconfined`, as the repository's compose files do, because Docker's default profile blocks io_uring, and `IGGY_SHARDING_CPU_ALLOCATION=2` replaces the default NUMA allocation, which fails in Docker Desktop's VM (apache/iggy#4242).

## Iggy Rust SDK demo

The Iggy Rust SDK tab is `rust-demo`, which runs the Iggy SDK demo's workload on the Iggy Rust SDK (`iggy` 0.11.0, released with server 0.9.0). It serves the same page, takes the same settings and reports the same numbers, so the two SDKs can be compared side by side.

It is a separate process, on http://localhost:8081 by default. `--port`, `--bootstrap` and `--max-topic-mib` set what `-Ddemo.port`, `-Ddemo.bootstrap` and `-Ddemo.maxTopicMiB` set for the Java demos, and `IGGY_PASSWORD` works the same way.

Producers and consumers are Tokio tasks, each with its own connection. A consumer polls with no partition given, as in the Java demo. The Rust SDK picks the next partition from the member's cached assignment, so the page asks the server for each consumer's partitions with the same SyncConsumerGroup request the SDK uses.

Rust has no garbage collector, so the GC column and markers stay empty.

## Benchmark script

`scripts/demo-bench.sh` runs one scenario for a fixed time against either demo (`CLIENT=kafka` or `CLIENT=iggy`), prints the averages and appends a line to `bench-results.jsonl`. Settings are environment variables, listed at the top of the script.

## Building and testing

```bash
mvn test
```

The tests start `apache/iggy:0.9.0` with Testcontainers, so Docker must be running. `mvn package` runs them and builds the jar into `target`.

To run the tests with `kafka-clients` 3.9 in place of 4.3:

```bash
mvn test -Pkafka3
```

A benchmark runs the same producer and consumer code against Iggy through this library and against Kafka 4.3, one broker at a time. It needs the `apache/iggy` and `apache/kafka` images, takes a few minutes, and is left out of `mvn test`:

```bash
mvn test -Pbenchmark
```

Java code is formatted with Palantir Java Format through Spotless, with the same rules as the Iggy Java SDK. CI fails on unformatted code. To format before checking in:

```bash
mvn spotless:apply
```
