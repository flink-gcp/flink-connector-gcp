# Bigtable module guidance

Read `.agents/references/modules/flink-connector-gcp-bigtable.md` and its linked ADRs before
changing behavior or public API. Preserve the client flow-control assumptions, row/fatal failure
boundary, batching weights, add-only table repair, stalled-wait reporting, bounded scan source,
Table API mapping, and emulator/real-service distinction. Run the relevant real-GCP suite for
service behavior and keep cleanup/sweep behavior aligned with provisioning changes.
