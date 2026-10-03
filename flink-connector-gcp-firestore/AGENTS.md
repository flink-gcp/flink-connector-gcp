# Firestore module guidance

Read `.agents/references/modules/flink-connector-gcp-firestore.md` and its linked ADRs before
changing behavior or public API. Preserve the two package roots (`connector.firestore` for Native
mode, `connector.datastore` for Datastore mode) sharing nothing beyond the base module, the
BulkWriter-based at-least-once sink, its routing boundary and solo confirmation, and the two
BulkWriter defect workarounds; and for the Datastore-mode sink, the flush before a repeated key,
the single-attempt client under the writer's own retry loop, and the lookup before a `NOT_FOUND`
is routed. Update the Firestore documentation with behavior changes, and do not
settle a mapping or behavior on emulator evidence alone.
