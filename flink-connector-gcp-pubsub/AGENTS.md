# Pub/Sub module guidance

Read `.agents/references/modules/flink-connector-gcp-pubsub.md` and its linked ADRs before changing
behavior or public API. Preserve adapted-code provenance, sink ordering/backpressure and repair
rules, dead-letter behavior, source checkpoint/assignment contracts, metrics, Table API/SQL, and
uber-jar relocation/licensing. Keep emulator-backed tests on production factories and update the
matching Pub/Sub documentation with behavior changes.
