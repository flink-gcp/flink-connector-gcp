# Bigtable Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the workload of the Bigtable recovery trials that [issue #1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [issue #1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317) share, as their [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1316#issuecomment-6247061185) decided.
One job graph carries all three components the trials recover.
A finite, checkpointed input reaches the production `BigtableSink` in both delivery modes, and the production `BigtableChangeStreamSource` relays what the `EXACTLY_ONCE` sink applied.
Every connector uses the native transport and the workload's application default credentials; no emulator endpoint can be configured.

## Build and local validation

```sh
mise x -- just tier3-bigtable-verify
```

The opt-in `tier3-bigtable` Maven profile keeps the application outside published connector artifacts.
The build produces `target/bigtable-recovery.jar` and copies runtime dependencies into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.bigtable.BigtableRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `bigtable-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base, and the application CI lane builds the Dockerfile without publishing.

Unit tests cover argument and input bounds, the row keys and contributions both sinks write, the relay's deserializer and delivery cells, and refusal of state from another trial.
A MiniCluster test runs the graph with in-memory connectors through savepoint upgrades at parallelism 2→2, 2→1 and 1→2, and requires every record at each connector afterwards.
Another builds the graph with the production connectors and pins its operator UIDs, vertex names, parallelisms and max parallelism.
A third pins each production connector through its public surface: sink class and mode, app profile, marker family, the mutation its serializer writes, its table, and the source's boundedness; the source's start position is not visible outside the connector's package.

The real connectors do not run locally.
The staged sink reads its app profile through the instance admin API, which the Bigtable emulator does not serve, and the change-stream source has no emulator endpoint.
The connector module's own tests cover most of their behavior, including a staged `AddToCell` commit against the service.
None of its change-stream tests reads an aggregate `AddToCell` change from the service, so the deployed trials are the first run of that path, as of the whole graph.

## Instance layout

The rig's runner creates one instance per run in project `flink-gcp`, named `bt1316-` followed by a run label, at most 33 characters, and passes it as `--instance`.
The application expects these resources in it:

| Resource | Contents |
| --- | --- |
| App profile `recovery` | Single-cluster routing with single-row transactions allowed. The staged sink requires it, and the change-stream source reads its partitions under it. |
| Table `eo` | Family `agg` of type INT64 SUM, family `flink_commit` raw without a GC rule (the staged sink's markers), and a change stream with 24 hours' retention. Initial splits at `4`, `8` and `c` give the source at least four partitions, as the gated Change Streams test measures. |
| Table `alo` | Family `agg` of type INT64 SUM. |
| Table `relay` | Family `r`. |

## Graph and what each table records

Record `n` of `--records` has the row key `<n mod 16 in hex>/<n, seven digits>`, so consecutive records spread over every initial partition of `eo`.
The input runs at parallelism one; every other vertex runs at `--parallelism`, one or two.

- **`EXACTLY_ONCE` sink into `eo`:** each record adds 1 to `agg:count` at timestamp 1000 µs, and the staged commit writes the contribution and one marker in a single conditional write. Each row's SUM is therefore 1 and it has exactly one marker.
- **`AT_LEAST_ONCE` sink into `alo`:** the same contribution without staging. A replay after a restore adds to the SUM again, and so does a client retry of an entry whose first attempt was applied, so a SUM above 1 counts extra contributions without naming their cause.
- **Change-stream relay from `eo` into `relay`:** for every change that adds to `agg`, the relay writes the row key with one cell in family `r`. Its qualifier is `<phase>/<uuid>/<n>`: the phase, a UUID drawn when the task instance opens, and the delivery's position in that instance. Its value is the change's commit time in microseconds. A change the source delivers again, whether after a restore or within one instance (as when the connector restarts a partition that lost its continuation token), therefore adds a qualifier, and the number of qualifiers in a relay row counts its deliveries. A change without a contribution, such as one holding only markers, is skipped; a row outside the input is relayed for the oracle to report.

The source starts at `--change-stream-start` on a fresh start and stops at `--change-stream-end`, so the job finishes once the input is complete and the source reaches its end.
The connector rejects a start later than the JobManager's clock, so the rig passes an instant it has already passed, such as the table's creation.
The source signals its end only when no partition is missing, so a partition the connector is still reconciling holds the job past the end.
The connector recovers a gap 2 minutes after it first observes it when pending merges' continuation tokens cover it and 20 minutes after when they do not, and then replays it; neither grace is measured from the end, so neither bounds when the job finishes.
A restored partition whose position has fallen outside the table's retention fails the job rather than skipping ahead; the connector's restart of a partition that lost its token is instead moved forward to a minute inside the retention, which a position from a trial of a few hours never needs.
A change committed after the end is not relayed, so the rig chooses an end after the input's last commit and judges the relay oracle only when the input finished before it.

Flink chains the graph into four vertices, whose names the rig's vertex selection reads:

| Vertex | Parallelism |
| --- | --- |
| `Source: Bigtable input -> Bigtable input identity` | 1 |
| `Bigtable EXACTLY_ONCE: Writer -> Bigtable EXACTLY_ONCE: Committer` | `--parallelism` |
| `Bigtable AT_LEAST_ONCE: Writer` | `--parallelism` |
| `Source: Bigtable change stream -> Bigtable relay delivery -> Bigtable relay: Writer` | `--parallelism` |

Progress lines use the `bigtable-progress` event in the shape the shared recovery exercise parses; snapshot and checkpoint-complete lines use the `bigtable-snapshot` and `bigtable-checkpoint-complete` events.

## Arguments

| Argument | Meaning |
| --- | --- |
| `--run-id` | The Tier-3 run label. |
| `--instance` | The run's instance ID, `bt1316-` followed by a label. |
| `--records` | Records in the input, 2 to 360,000. |
| `--records-per-second` | Offered input rate, 1 to 100. The staged committers apply each checkpoint's input in one burst. |
| `--change-stream-start`, `--change-stream-end` | ISO-8601 instants; the end follows the start by at most the 24 hours of retention. |
| `--phase` | `initial` or `upgrade`. |
| `--require-restored` | `true` refuses a fresh start; `upgrade` requires it. |
| `--parallelism` | Subtasks of every vertex but the input, 1 or 2 (default 2). |

The run, instance, input and change-stream span form the trial identity that the restored input state must match; the phase and parallelism may change in an upgrade.
The check runs only while the input is active: once the input has finished, Flink restores its vertex as finished without initializing its state, so a later restore is not checked.
The trials' recoveries all complete while the input runs, because the shared exercise fails a run whose input finishes before its post-recovery window.
The grants, the CUE delivery, the scenario and its oracles belong to [#1853](https://github.com/flink-gcp/flink-connector-gcp/issues/1853) and [#1854](https://github.com/flink-gcp/flink-connector-gcp/issues/1854).
