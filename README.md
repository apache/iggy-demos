# Apache Iggy demos

Demo applications and use cases built on [Apache Iggy](https://iggy.apache.org), one folder per demo. Each folder has its own README with what it shows and how to run it. Demos build against a released Iggy server, not the development branch.

## Demos

- [kafka-clients](kafka-clients/): Kafka's `Producer`, `Consumer` and `Admin` interfaces backed by Iggy, with a demo page that shows messages moving in real time and what happens when producers outrun a consumer, and the same workload on the Iggy Rust SDK for comparison.

## Adding a demo

Open a pull request that adds a folder with a README, the code and whatever it needs to run. Say which Iggy server version it was run against. Code goes under the Apache License 2.0 with the standard header on every source file; the header check runs on every pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
