/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink;
import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link BigQueryDynamicTableFactory}. */
class BigQueryDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()));

    /** {@link #SCHEMA} plus a column {@code sink.table-create.*} can partition on. */
    private static final ResolvedSchema PARTITIONABLE =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()),
                    Column.physical("event_ts", DataTypes.TIMESTAMP_LTZ(6)));

    /** The destination {@link #minimalOptions()} names, for reading creation options back. */
    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static Map<String, String> minimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", BigQueryDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("dataset", "my_dataset");
        options.put("table", "my_table");
        return options;
    }

    private static DynamicTableSink sink(Map<String, String> options) {
        return FactoryMocks.createTableSink(SCHEMA, options);
    }

    /**
     * The connector's own sink, as the planner would obtain it.
     *
     * <p>Every option assertion reads off this rather than off the {@link DynamicTableSink}: a
     * value dropped on the way to {@code BigQuerySink.builder()} is invisible everywhere else.
     */
    private static Sink<?> built(Map<String, String> options) {
        return built(SCHEMA, options);
    }

    private static Sink<?> built(ResolvedSchema schema, Map<String, String> options) {
        return ((SinkV2Provider)
                        FactoryMocks.createTableSink(schema, options)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .createSink();
    }

    /** {@link #minimalOptions()} plus the write method and whatever that method requires. */
    private static Map<String, String> optionsFor(WriteMethod writeMethod) {
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", writeMethod.toString());
        if (writeMethod == WriteMethod.FILE_LOADS) {
            options.put("sink.file-loads.staging-path", "gs://bucket/prefix");
        }
        return options;
    }

    @Test
    void buildsASinkFromTheMinimalOptions() {
        assertThat(sink(minimalOptions()))
                .isInstanceOf(BigQueryDynamicSink.class)
                .extracting(DynamicTableSink::asSummaryString)
                .isEqualTo("BigQuery table sink");
    }

    @Test
    void theSinkItBuildsIsTheConnectorsOwn() {
        SinkV2Provider provider =
                (SinkV2Provider)
                        sink(minimalOptions())
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        // Without sink.write-method, the connector's own default write method.
        assertThat(provider.createSink()).isInstanceOf(BigQueryDefaultStreamSink.class);
        assertThat(provider.getParallelism()).isEmpty();
    }

    @Test
    void carriesTheSinkParallelismWhenItIsSet() {
        Map<String, String> options = minimalOptions();
        options.put("sink.parallelism", "3");
        SinkV2Provider provider =
                (SinkV2Provider)
                        sink(options).getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider.getParallelism()).hasValue(3);
    }

    @Test
    void rejectsATableWithoutItsThreeDestinationParts() {
        for (String missing : new String[] {"project", "dataset", "table"}) {
            Map<String, String> options = minimalOptions();
            options.remove(missing);
            assertThatThrownBy(() -> sink(options))
                    .as("without '%s'", missing)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(missing);
        }
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalOptions();
        // A near miss of a real key, which is how one is usually written.
        options.put("sink.write_method", "storage-api-at-least-once");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.write_method");
    }

    @Test
    void everyWriteMethodBuildsItsOwnSink() {
        // Also pins the DDL spelling against the sink it selects: a WriteMethod whose toString()
        // drifted would build the wrong one of these rather than fail.
        assertThat(built(optionsFor(WriteMethod.STORAGE_API_AT_LEAST_ONCE)))
                .isInstanceOf(BigQueryDefaultStreamSink.class);
        assertThat(built(optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE)))
                .isInstanceOf(BigQueryBufferedStreamSink.class);
        assertThat(built(optionsFor(WriteMethod.FILE_LOADS)))
                .isInstanceOf(BigQueryFileLoadsSink.class);
    }

    @Test
    void aWriteMethodThatTunesNothingStillGetsItsRequiredOptions() {
        // The reason the two required families are built from the write method rather than from
        // key presence: the builder demands the object, the DDL is correct, and there is no key to
        // trigger a presence scan. Every knob is then the connector's own default.
        assertThat(
                        ((BigQueryBufferedStreamSink<?>)
                                        built(optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE)))
                                .getOptions())
                .isEqualTo(BufferedStreamOptions.builder().build());
        assertThat(
                        ((BigQueryFileLoadsSink<?>) built(optionsFor(WriteMethod.FILE_LOADS)))
                                .getOptions())
                .isEqualTo(FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build());
    }

    @Test
    void bufferedStreamKeysReachTheBuiltSink() {
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.buffered-stream.retry.max-attempts", "7");
        options.put("sink.buffered-stream.max-append-request-bytes", "1 mb");
        BufferedStreamOptions built = ((BigQueryBufferedStreamSink<?>) built(options)).getOptions();
        assertThat(built.getRetryMaxAttempts()).isEqualTo(7);
        assertThat(built.getMaxAppendRequestBytes()).isEqualTo(1024L * 1024L);
    }

    @Test
    void fileLoadsKeysReachTheBuiltSink() {
        Map<String, String> options = optionsFor(WriteMethod.FILE_LOADS);
        options.put("sink.file-loads.temp-dataset", "staging_dataset");
        options.put("sink.file-loads.write-disposition", "write-truncate");
        options.put("sink.file-loads.schema-reconcile.max-attempts", "3");
        FileLoadsOptions built = ((BigQueryFileLoadsSink<?>) built(options)).getOptions();
        assertThat(built.getStagingPath()).isEqualTo("gs://bucket/prefix");
        assertThat(built.getTempDataset()).isEqualTo("staging_dataset");
        assertThat(built.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_TRUNCATE);
        // The keys follow the setters (schema-reconcile.*), the getters say schemaUpdate.
        assertThat(built.getSchemaUpdateMaxAttempts()).isEqualTo(3);
    }

    @Test
    void aTuningKeyOfAnotherWriteMethodIsRejectedByKeyName() {
        // Each family under each write method that does not own it — six cases, the whole matrix,
        // because a check written for one family is a check that could have missed the others. The
        // builder rejects the pair too, naming bufferedStreamOptions(...): a method a SQL user
        // cannot call.
        Map<WriteMethod, String> familyKeys = new LinkedHashMap<>();
        familyKeys.put(
                WriteMethod.STORAGE_API_AT_LEAST_ONCE, "sink.default-stream.max-inflight-requests");
        familyKeys.put(
                WriteMethod.STORAGE_API_EXACTLY_ONCE, "sink.buffered-stream.retry.max-attempts");
        familyKeys.put(WriteMethod.FILE_LOADS, "sink.file-loads.schema-reconcile.max-attempts");

        familyKeys.forEach(
                (owner, key) -> {
                    for (WriteMethod selected : WriteMethod.values()) {
                        if (selected == owner) {
                            continue;
                        }
                        Map<String, String> options = optionsFor(selected);
                        options.put(key, "7");
                        assertThatThrownBy(() -> sink(options))
                                .as("'%s' under '%s'", key, selected)
                                .isInstanceOf(ValidationException.class)
                                // A phrase only this connector's message carries. FactoryUtil
                                // attaches a dump of the whole WITH clause to anything the factory
                                // throws, so asserting the key alone would pass with the check
                                // deleted — measured.
                                .hasStackTraceContaining("but this table's write method is")
                                .hasStackTraceContaining(key);
                    }
                });
    }

    @Test
    void aTuningKeyIsRejectedWhenNoWriteMethodIsNamedEither() {
        // The case the matrix above cannot reach, because it always writes sink.write-method: a
        // table that names no write method is on the connector's default, and a key of another
        // family is as wrong there as anywhere. Without this a check that simply returned when the
        // option was absent would pass the whole suite — and the keys would then be dropped in
        // silence, since the sink only builds a family whose write method matches.
        Map<String, String> options = minimalOptions();
        options.put("sink.buffered-stream.retry.max-attempts", "7");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("but this table's write method is")
                .hasStackTraceContaining("sink.buffered-stream.retry.max-attempts");
    }

    @Test
    void aMissingStagingPathUnderFileLoadsIsRejectedByKeyName() {
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", "file-loads");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no default location to stage them in")
                .hasStackTraceContaining("sink.file-loads.staging-path");
    }

    @Test
    void theWriteMethodBeingUnusableIsReportedAheadOfWhatIsConfiguredUnderIt() {
        // Ordering, and it is load-bearing: TableCreateOptionsMapper throws too, and while the
        // staging-path check sat inside the builder chain it was evaluated first — so a FILE_LOADS
        // table with nowhere to stage was told about its create disposition instead.
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", "file-loads");
        options.put("sink.create-disposition", "create-never");
        options.put("sink.table-create.clustered-fields", "id");

        Throwable thrown = catchThrowable(() -> sink(options));

        assertThat(thrown).isInstanceOf(ValidationException.class);
        assertThat(Throwables.getStackTrace(thrown))
                .contains("no default location to stage them in")
                .doesNotContain("configure a table this sink never creates");
    }

    @Test
    void schemaEvolutionUnderExactlyOnceIsRejectedByKeyName() {
        // A buffered stream's schema is pinned at stream creation. The builder says so naming
        // schemaUpdateOptions(...); this says it in keys.
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.schema-update.allow-field-relaxation", "true");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                // The opening clause, not "pinned when the stream is created": the builder's own
                // message carries that phrase verbatim, so it would not tell the two apart in a
                // test that reaches the builder — as the planner-level sibling does.
                .hasStackTraceContaining("ask the sink to evolve the table schema")
                .hasStackTraceContaining("sink.schema-update.allow-field-relaxation");
    }

    @Test
    void aSchemaUpdateKeySetToFalseIsAcceptedUnderExactlyOnce() {
        // The check fires on the same condition the builder uses — an *enabled* options object —
        // so a key present and false is no more a schema update here than it is there. Without
        // this the check could tighten to mere presence and nothing would notice.
        Map<String, String> options = optionsFor(WriteMethod.STORAGE_API_EXACTLY_ONCE);
        options.put("sink.schema-update.allow-new-fields", "false");
        assertThat(built(options)).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void anEmulatorEndpointUnderFileLoadsIsRejectedByKeyName() {
        for (String key : new String[] {"emulator-endpoint", "emulator-rest-endpoint"}) {
            Map<String, String> options = optionsFor(WriteMethod.FILE_LOADS);
            options.put(key, "localhost:9060");
            assertThatThrownBy(() -> sink(options))
                    .as("with '%s'", key)
                    .isInstanceOf(ValidationException.class)
                    // Again the opening clause: the builder says "which the BigQuery emulator
                    // does not provide", one word away from the tail of this one.
                    .hasStackTraceContaining("point at a BigQuery emulator")
                    .hasStackTraceContaining(key);
        }
    }

    @Test
    void rejectsAnUpdatingQueryByBeingInsertOnly() {
        assertThat(
                        sink(minimalOptions())
                                .getChangelogMode(
                                        org.apache.flink.table.connector.ChangelogMode.all()))
                .isEqualTo(org.apache.flink.table.connector.ChangelogMode.insertOnly());
    }

    @Test
    void aDefaultStreamKeyReachesTheBuiltSink() {
        Map<String, String> options = minimalOptions();
        options.put("sink.default-stream.max-inflight-requests", "7");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getOptions().getMaxInflightRequests()).isEqualTo(7);
    }

    @Test
    void theSchemaAndEmulatorOptionsReachTheBuiltSink() {
        Map<String, String> options = minimalOptions();
        options.put("sink.location", "asia-northeast1");
        options.put("sink.create-disposition", "create-never");
        options.put("emulator-endpoint", "localhost:9060");
        options.put("emulator-rest-endpoint", "localhost:9050");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(built.getConfig().getCreateDisposition().name()).isEqualTo("CREATE_NEVER");
        assertThat(built.getConfig().getEmulatorEndpoint().getTarget()).isEqualTo("localhost:9060");
        assertThat(built.getConfig().getEmulatorRestEndpoint().getTarget())
                .isEqualTo("localhost:9050");
    }

    @Test
    void schemaUpdateKeysReachTheBuiltSinkAndTheirAbsenceLeavesTheDefault() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>) built(minimalOptions());
        assertThat(defaults.getConfig().getSchemaUpdateOptions().isEnabled()).isFalse();

        Map<String, String> options = minimalOptions();
        options.put("sink.schema-update.allow-new-fields", "true");
        BigQueryDefaultStreamSink<?> built = (BigQueryDefaultStreamSink<?>) built(options);
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowNewFields()).isTrue();
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowFieldRelaxation()).isFalse();
    }

    @Test
    void tableCreateKeysReachTheBuiltSinkAndTheirAbsenceLeavesAPlainTable() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>) built(minimalOptions());
        assertThat(defaults.getConfig().getTableCreateOptionsProvider().optionsFor(DESTINATION))
                .isEqualTo(TableCreateOptions.defaults());

        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.time-partitioning.type", "day");
        options.put("sink.table-create.time-partitioning.field", "event_ts");
        options.put("sink.table-create.clustered-fields", "id");
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>) built(PARTITIONABLE, options);
        TableCreateOptions created =
                built.getConfig().getTableCreateOptionsProvider().optionsFor(DESTINATION);
        assertThat(created.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(created.getTimePartitioningField()).isEqualTo("event_ts");
        assertThat(created.getClusteredFields()).containsExactly("id");
    }

    @Test
    void aTableCreateColumnOutsideTheDdlIsRejected() {
        // The check only this layer can make: the emulator would accept the create request and
        // real BigQuery would refuse it, so a plan-time failure is what keeps the two apart.
        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.clustered-fields", "no_such_column");
        assertThatThrownBy(() -> sink(options))
                .isInstanceOf(ValidationException.class)
                // A phrase only the connector's own message carries. Asserting the option key or
                // the column name would pass with the check deleted, because FactoryUtil dumps
                // every WITH option into the ValidationException it wraps this in — measured.
                .hasStackTraceContaining("which the table does not declare")
                .hasStackTraceContaining("sink.table-create.clustered-fields")
                .hasStackTraceContaining("no_such_column");
    }

    @Test
    void aSchemaProblemFailsWhenTheJobGraphIsBuilt() {
        // The eager-derivation rule: an unmappable column must not wait until serialize() runs
        // inside the writers' failure handler.
        ResolvedSchema unmappable =
                ResolvedSchema.of(Column.physical("v", DataTypes.INTERVAL(DataTypes.DAY())));
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSink(unmappable, minimalOptions())
                                        .getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
    }
}
