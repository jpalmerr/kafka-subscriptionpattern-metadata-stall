# Java regex consumers and Metadata by topic id 

## Summary

A Java consumer on kafka-clients 4.1 or later, using group.protocol=consumer and subscribe(SubscriptionPattern),
throws IllegalStateException("Use errorsByTopicId() when managing topic using topic id") when a topic assigned to it is deleted.

- Since 4.1, this consumer asks for metadata by topic ID.
- The broker answers a deleted topic's ID with `UNKNOWN_TOPIC_ID` and no name, which the schema allows from v12. 
- `NetworkClient` then reads the errors by topic name, and `Metadata.errors()` throws on the row without a name.

As a consequence, the client never marks the Metadata request as finished, so it sends no more Metadata requests and never consumes new matching topics.
With auto-commit disabled, we also saw it send `ConsumerGroupHeartbeat` continuously once a new matching topic was assigned.

## Reproduce 

To reproduce, you need: 

- Docker
- Java 17+
- curl

This script runs the scenario against a real broker, for kafka-clients versions before and after the change, side by side:

1. Starts one Kafka node in Docker (the official `apache/kafka:4.3.1` image), with default settings except `auto.create.topics.enable=false`.
2. Creates `stall-a` and `stall-b` and produces to both.
3. Starts five consumers, each in its own group, using `StallProbe.java` with kafka-clients jars downloaded from Maven Central:
   - `regex-4.0.0`, `regex-4.1.0`, `regex-4.2.0`, `regex-4.3.1`: `subscribe(new SubscriptionPattern("stall-.*"))`
   - `names-4.1.0`: `subscribe(List.of("stall-a", "stall-b"))`, as a control
4. After 45 s, deletes `stall-b`. After 30 s more, creates `stall-c`, which matches the regex, and produces to it.
5. Watches for 120 s, then prints each consumer's exception and a verdict table.

Auto-commit is off by default so the heartbeat loop shows; the stall itself happens with it on too. Jars and logs go under `$TMPDIR/kafka-regex-stall`, and the containers are removed on exit. A run takes about 4 minutes.

Download the files on this page (Download ZIP, or clone the gist), then from that folder:

```
bash reproduce.sh
```

For each consumer, it prints:

- whether it threw the `IllegalStateException`
- how many Metadata updates it made after `stall-b` was deleted
- whether it consumed `stall-c`
- its heartbeats per second

The 4.1+ regex consumers throw, make no Metadata updates, never consume `stall-c`, and heartbeat thousands of times a second. The 4.0.0 regex consumer and the name-subscribed consumer keep working.

Options: `--brokers 3` (a separate controller and three brokers), `--broker-version <version>`, `--observe <seconds>`, `--auto-commit true`, `--out <dir>`.