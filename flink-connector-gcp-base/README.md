# Flink Connectors GCP : Base

Shared main-code infrastructure for the connector modules
([#61](https://github.com/laughingman7743/flink-connector-gcp/issues/61)): the retry schedule and
backoff-sleep helper behind the connectors' recovery loops, gRPC status-code extraction from the
exceptions the Google Cloud clients surface, and — from the DLQ and metrics standardization
([#37](https://github.com/laughingman7743/flink-connector-gcp/issues/37)) — the cross-connector
failure-handling SPI (`FailureHandler`, `DeadLetterQueue`) plus the sink metric helpers the
connectors register their error-class and per-destination counters through. `base.lifecycle` holds
the close-time helpers: releasing several resources so that one refusing to close never strands the
rest, and bounding a client shutdown that is not guaranteed to return.

Every type in this module is `@Internal` except the failure-handling SPI in `base.failure`, which
is public because users implement it; the connectors' own builders remain the place failure
policies are configured. Everything else is consumed by the sibling connector modules at compile
scope, is not part of any connector's public API, and carries no compatibility guarantee outside
this repository. Retry behavior is configured through each connector's own public options
objects, which map onto the internal schedule type here.
