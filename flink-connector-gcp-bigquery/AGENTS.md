# BigQuery module guidance

Read `.agents/references/modules/flink-connector-gcp-bigquery.md` and its linked ADRs before
changing behavior or public API. Preserve the serializer facade, per-write-method validation,
failure/recovery boundaries, FILE_LOADS topology, exactly-once protocol, Table API/SQL packaging,
source split semantics, and metrics contracts. Update the BigQuery design, reference, quickstart,
or example page when the corresponding behavior changes; use the real-GCP suite for service
semantics the emulator cannot establish.
