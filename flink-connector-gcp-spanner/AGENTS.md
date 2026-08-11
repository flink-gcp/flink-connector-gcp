# Spanner module guidance

Read `.agents/references/modules/flink-connector-gcp-spanner.md` and its linked ADRs before changing
behavior or public API. Preserve mutation-based at-least-once delivery, retry/routing policy,
index-aware batch weights, dialect support, server-partitioned bounded source semantics, and
real-GCP test provisioning/cleanup. Update the Spanner documentation with behavior changes and use
the gated suite where emulator behavior is insufficient.
