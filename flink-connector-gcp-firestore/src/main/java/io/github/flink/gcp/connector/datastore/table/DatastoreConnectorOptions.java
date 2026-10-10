/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code datastore} table connector.
 *
 * <p>A mapped option is declared without a default: its default lives on the connector's own
 * builder and is applied by not calling the setter. A test records the exception, the table-owned
 * {@code type-mismatch-policy} the factory reads with {@code get()}. No description restates a
 * default: the reference and table docs pages carry each default, and a test rejects the
 * restatement.
 */
@PublicEvolving
public final class DatastoreConnectorOptions {

    private DatastoreConnectorOptions() {}

    /** The Google Cloud project containing the database in Datastore mode. */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project containing the database in Datastore mode.");

    /** The id of the Firestore database in Datastore mode. */
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The id of the Firestore database in Datastore mode.");

    /** The kind whose entities are the table's rows. */
    public static final ConfigOption<String> KIND =
            ConfigOptions.key("kind")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The kind whose entities are the table's rows.");

    /** The namespace of the table's entities; absent for the default namespace. */
    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The namespace of the table's entities; absent for the default"
                                    + " namespace.");

    /** The host:port of a Firestore emulator started in Datastore mode. */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The host:port of a Firestore emulator started in Datastore mode.");

    /** The service-account JSON key-file path available to each runtime process. */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The service-account JSON key-file path available to each runtime"
                                    + " process.");

    /**
     * What a read does with a stored value whose type does not match its column: 'fail' the read,
     * or read the field as 'null'.
     */
    public static final ConfigOption<TypeMismatchPolicy> TYPE_MISMATCH_POLICY =
            ConfigOptions.key("type-mismatch-policy")
                    .enumType(TypeMismatchPolicy.class)
                    .defaultValue(TypeMismatchPolicy.FAIL)
                    .withDescription(
                            "What a read does with a stored value whose type does not match its"
                                    + " column: 'fail' the read, or read the field as 'null'.");

    /** The desired maximum number of key ranges a scan is split into. */
    public static final ConfigOption<Integer> SCAN_PARTITION_MAX_PARTITIONS =
            ConfigOptions.key("scan.partition.max-partitions")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The desired maximum number of key ranges a scan is split into.");

    /** An ISO-8601 instant at which every split of the scan reads. */
    public static final ConfigOption<String> SCAN_READ_TIME =
            ConfigOptions.key("scan.read-time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("An ISO-8601 instant at which every split of the scan reads.");

    /** How many entities one request of the scan asks for. */
    public static final ConfigOption<Integer> SCAN_MAX_ROWS_PER_FETCH =
            ConfigOptions.key("scan.max-rows-per-fetch")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many entities one request of the scan asks for.");

    /**
     * Whether the lookup join runs as Flink's asynchronous lookup, keeping several reads in flight
     * per subtask instead of waiting for each.
     */
    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the lookup join runs as Flink's asynchronous lookup, keeping"
                                    + " several reads in flight per subtask instead of waiting"
                                    + " for each.");

    /**
     * Top-level columns whose values, with every value nested in them, the sink writes excluded
     * from Datastore's indexes, which refuse a string or a blob longer than 1,500 bytes.
     */
    public static final ConfigOption<List<String>> SINK_UNINDEXED_COLUMNS =
            ConfigOptions.key("sink.unindexed-columns")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Top-level columns whose values, with every value nested in them, the"
                                    + " sink writes excluded from Datastore's indexes, which"
                                    + " refuse a string or a blob longer than 1,500 bytes.");

    /** The maximum mutations in one commit. */
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_MUTATIONS =
            ConfigOptions.key("sink.buffer-flush.max-mutations")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The maximum mutations in one commit.");

    /** The maximum protobuf size of one commit request, at most 10 MiB. */
    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum protobuf size of one commit request, at most 10 MiB.");

    /** The timeout of one call the writer makes: a commit attempt, or an id allocation. */
    public static final ConfigOption<Duration> SINK_REQUEST_TIMEOUT =
            ConfigOptions.key("sink.request-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The timeout of one call the writer makes: a commit attempt, or an id"
                                    + " allocation.");

    /** The first backoff of the writer's retry loop. */
    public static final ConfigOption<Duration> SINK_RECOVERY_INITIAL_BACKOFF =
            ConfigOptions.key("sink.recovery.initial-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The first backoff of the writer's retry loop.");

    /** The longest backoff of the writer's retry loop. */
    public static final ConfigOption<Duration> SINK_RECOVERY_MAX_BACKOFF =
            ConfigOptions.key("sink.recovery.max-backoff")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The longest backoff of the writer's retry loop.");

    /** How many attempts the writer's retry loop makes at one commit, the first included. */
    public static final ConfigOption<Integer> SINK_RECOVERY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.recovery.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many attempts the writer's retry loop makes at one commit, the"
                                    + " first included.");

    /**
     * How many ids one id allocation fetches, for a table without a PRIMARY KEY, whose rows are
     * written under ids the service allocates.
     */
    public static final ConfigOption<Integer> SINK_ID_ALLOCATION_BATCH_SIZE =
            ConfigOptions.key("sink.id-allocation.batch-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many ids one id allocation fetches, for a table without a PRIMARY"
                                    + " KEY, whose rows are written under ids the service"
                                    + " allocates.");

    /** Whether the writer paces itself with Datastore's ramp-up guidance. */
    public static final ConfigOption<Boolean> SINK_THROTTLING_ENABLED =
            ConfigOptions.key("sink.throttling.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the writer paces itself with Datastore's ramp-up guidance.");

    /**
     * How many subtasks share the ramp-up's starting budget of 500 operations per second; set it
     * above the sink's parallelism when other writers share the database.
     */
    public static final ConfigOption<Integer> SINK_THROTTLING_PARALLELISM =
            ConfigOptions.key("sink.throttling.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many subtasks share the ramp-up's starting budget of 500"
                                    + " operations per second; set it above the sink's"
                                    + " parallelism when other writers share the database.");
}
