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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;
import java.util.List;

/**
 * The {@code WITH} options of the {@code firestore} table connector.
 *
 * <p>A mapped option is declared without a default: its default lives on the connector's own
 * builder and is applied by not calling the setter. A test records the exceptions, the table-owned
 * selectors the factory reads with {@code get()}. No description restates a default: the reference
 * and table docs pages carry each default, and a test rejects the restatement.
 */
@PublicEvolving
public final class FirestoreConnectorOptions {

    private FirestoreConnectorOptions() {}

    /** The Google Cloud project containing the Firestore database. */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Google Cloud project containing the Firestore database.");

    /** The id of the Firestore database in Native mode. */
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The id of the Firestore database in Native mode.");

    /**
     * The path of the collection whose documents are the table's rows, relative to the database: a
     * collection id, or a path such as 'users/alice/orders' for a subcollection.
     */
    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The path of the collection whose documents are the table's rows,"
                                    + " relative to the database: a collection id, or a path such"
                                    + " as 'users/alice/orders' for a subcollection.");

    /** The host:port of a Firestore emulator. */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host:port of a Firestore emulator.");

    /** The service-account JSON key-file path available to each runtime process. */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The service-account JSON key-file path available to each runtime"
                                    + " process.");

    /**
     * Field paths of ROW values with the DOUBLE fields latitude and longitude that are Firestore
     * geographical points, for example 'location' or 'stops.position'.
     */
    public static final ConfigOption<List<String>> GEO_POINT_FIELD_PATHS =
            ConfigOptions.key("geo-point-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Field paths of ROW values with the DOUBLE fields latitude and"
                                    + " longitude that are Firestore geographical points, for"
                                    + " example 'location' or 'stops.position'.");

    /**
     * Field paths of STRING values that are Firestore references, each a document path relative to
     * the database, for example 'author' or 'items.product'.
     */
    public static final ConfigOption<List<String>> REFERENCE_FIELD_PATHS =
            ConfigOptions.key("reference-field-paths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Field paths of STRING values that are Firestore references, each a"
                                    + " document path relative to the database, for example"
                                    + " 'author' or 'items.product'.");

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

    /**
     * Whether a scan reads every collection whose id is the last segment of 'collection', at any
     * depth of the database, as a partitioned collection-group scan, rather than the one collection
     * as a single split.
     */
    public static final ConfigOption<Boolean> SCAN_COLLECTION_GROUP =
            ConfigOptions.key("scan.collection-group")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether a scan reads every collection whose id is the last segment of"
                                    + " 'collection', at any depth of the database, as a"
                                    + " partitioned collection-group scan, rather than the one"
                                    + " collection as a single split.");

    /** The desired maximum number of partitions of a collection-group scan. */
    public static final ConfigOption<Integer> SCAN_PARTITION_MAX_PARTITIONS =
            ConfigOptions.key("scan.partition.max-partitions")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The desired maximum number of partitions of a collection-group scan.");

    /** An ISO-8601 instant at which every split of the scan reads. */
    public static final ConfigOption<String> SCAN_READ_TIME =
            ConfigOptions.key("scan.read-time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("An ISO-8601 instant at which every split of the scan reads.");

    /** How many documents one request of the scan asks for. */
    public static final ConfigOption<Integer> SCAN_MAX_ROWS_PER_FETCH =
            ConfigOptions.key("scan.max-rows-per-fetch")
                    .intType()
                    .noDefaultValue()
                    .withDescription("How many documents one request of the scan asks for.");

    /**
     * How a row with a PRIMARY KEY is written: 'set' replaces the document, 'merge' merges the row
     * into it, and 'update' replaces the table's fields of a document that must exist.
     */
    public static final ConfigOption<WriteMode> SINK_WRITE_MODE =
            ConfigOptions.key("sink.write-mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.SET)
                    .withDescription(
                            "How a row with a PRIMARY KEY is written: 'set' replaces the document,"
                                    + " 'merge' merges the row into it, and 'update' replaces the"
                                    + " table's fields of a document that must exist.");

    /** Whether the client library ramps the write rate up gradually. */
    public static final ConfigOption<Boolean> SINK_THROTTLING_ENABLED =
            ConfigOptions.key("sink.throttling.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the client library ramps the write rate up gradually.");

    /** The write rate the ramp-up starts at, in operations per second per sink subtask. */
    public static final ConfigOption<Integer> SINK_THROTTLING_INITIAL_OPS_PER_SECOND =
            ConfigOptions.key("sink.throttling.initial-ops-per-second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The write rate the ramp-up starts at, in operations per second per"
                                    + " sink subtask.");

    /** The write rate the ramp-up stops at, in operations per second per sink subtask. */
    public static final ConfigOption<Integer> SINK_THROTTLING_MAX_OPS_PER_SECOND =
            ConfigOptions.key("sink.throttling.max-ops-per-second")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The write rate the ramp-up stops at, in operations per second per"
                                    + " sink subtask.");

    /**
     * How many attempts the client library gives a write the service refuses with a retryable
     * status, the first included.
     */
    public static final ConfigOption<Integer> SINK_WRITE_MAX_ATTEMPTS =
            ConfigOptions.key("sink.write.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many attempts the client library gives a write the service"
                                    + " refuses with a retryable status, the first included.");

    /** The total time budget of one BatchWrite call including its transport retries. */
    public static final ConfigOption<Duration> SINK_RETRY_TOTAL_TIMEOUT =
            ConfigOptions.key("sink.retry.total-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The total time budget of one BatchWrite call including its transport"
                                    + " retries.");

    /** The delay before the first transport retry of a BatchWrite call. */
    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_DELAY =
            ConfigOptions.key("sink.retry.initial-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The delay before the first transport retry of a BatchWrite call.");

    /** The factor the transport retry delay grows by per attempt. */
    public static final ConfigOption<Double> SINK_RETRY_DELAY_MULTIPLIER =
            ConfigOptions.key("sink.retry.delay-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The factor the transport retry delay grows by per attempt.");

    /** The longest delay between two transport retries. */
    public static final ConfigOption<Duration> SINK_RETRY_MAX_DELAY =
            ConfigOptions.key("sink.retry.max-delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The longest delay between two transport retries.");

    /** The timeout of the first BatchWrite attempt. */
    public static final ConfigOption<Duration> SINK_RETRY_INITIAL_RPC_TIMEOUT =
            ConfigOptions.key("sink.retry.initial-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The timeout of the first BatchWrite attempt.");

    /** The factor the attempt timeout grows by per attempt. */
    public static final ConfigOption<Double> SINK_RETRY_RPC_TIMEOUT_MULTIPLIER =
            ConfigOptions.key("sink.retry.rpc-timeout-multiplier")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("The factor the attempt timeout grows by per attempt.");

    /** The longest timeout of one BatchWrite attempt. */
    public static final ConfigOption<Duration> SINK_RETRY_MAX_RPC_TIMEOUT =
            ConfigOptions.key("sink.retry.max-rpc-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The longest timeout of one BatchWrite attempt.");

    /** How many attempts the transport makes at one BatchWrite call. */
    public static final ConfigOption<Integer> SINK_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("sink.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "How many attempts the transport makes at one BatchWrite call.");

    /** The cap on writes submitted and not yet answered per sink subtask. */
    public static final ConfigOption<Integer> SINK_IN_FLIGHT_MAX_WRITES =
            ConfigOptions.key("sink.in-flight.max-writes")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on writes submitted and not yet answered per sink subtask.");

    /** The cap on the bytes of writes submitted and not yet answered per sink subtask. */
    public static final ConfigOption<MemorySize> SINK_IN_FLIGHT_MAX_BYTES =
            ConfigOptions.key("sink.in-flight.max-bytes")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "The cap on the bytes of writes submitted and not yet answered per"
                                    + " sink subtask.");
}
