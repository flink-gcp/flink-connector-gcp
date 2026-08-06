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
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.table.sink.BigQueryDynamicSink;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void acceptsTheOnlyWriteMethodThisLayerCarries() {
        Map<String, String> options = minimalOptions();
        options.put("sink.write-method", "storage-api-at-least-once");
        assertThat(sink(options)).isInstanceOf(BigQueryDynamicSink.class);
    }

    @Test
    void rejectsAWriteMethodThisLayerDoesNotCarryYet() {
        for (String method : new String[] {"storage-api-exactly-once", "file-loads"}) {
            Map<String, String> options = minimalOptions();
            options.put("sink.write-method", method);
            assertThatThrownBy(() -> sink(options))
                    .as("with '%s'", method)
                    .isInstanceOf(ValidationException.class)
                    // The message names the option key, which is what a SQL user can act on.
                    .hasStackTraceContaining("sink.write-method")
                    .hasStackTraceContaining(method);
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
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        sink(options)
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
        // Read off the built sink rather than the DynamicTableSink: a value dropped on the way to
        // the builder is invisible everywhere else.
        assertThat(built.getOptions().getMaxInflightRequests()).isEqualTo(7);
    }

    @Test
    void theSchemaAndEmulatorOptionsReachTheBuiltSink() {
        Map<String, String> options = minimalOptions();
        options.put("sink.location", "asia-northeast1");
        options.put("sink.create-disposition", "create-never");
        options.put("emulator-endpoint", "localhost:9060");
        options.put("emulator-rest-endpoint", "localhost:9050");
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        sink(options)
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
        assertThat(built.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(built.getConfig().getCreateDisposition().name()).isEqualTo("CREATE_NEVER");
        assertThat(built.getConfig().getEmulatorEndpoint().getTarget()).isEqualTo("localhost:9060");
        assertThat(built.getConfig().getEmulatorRestEndpoint().getTarget())
                .isEqualTo("localhost:9050");
    }

    @Test
    void schemaUpdateKeysReachTheBuiltSinkAndTheirAbsenceLeavesTheDefault() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        sink(minimalOptions())
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
        assertThat(defaults.getConfig().getSchemaUpdateOptions().isEnabled()).isFalse();

        Map<String, String> options = minimalOptions();
        options.put("sink.schema-update.allow-new-fields", "true");
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        sink(options)
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowNewFields()).isTrue();
        assertThat(built.getConfig().getSchemaUpdateOptions().isAllowFieldRelaxation()).isFalse();
    }

    @Test
    void tableCreateKeysReachTheBuiltSinkAndTheirAbsenceLeavesAPlainTable() {
        BigQueryDefaultStreamSink<?> defaults =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        sink(minimalOptions())
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
        assertThat(defaults.getConfig().getTableCreateOptionsProvider().optionsFor(DESTINATION))
                .isEqualTo(TableCreateOptions.defaults());

        Map<String, String> options = minimalOptions();
        options.put("sink.table-create.time-partitioning.type", "day");
        options.put("sink.table-create.time-partitioning.field", "event_ts");
        options.put("sink.table-create.clustered-fields", "id");
        BigQueryDefaultStreamSink<?> built =
                (BigQueryDefaultStreamSink<?>)
                        ((SinkV2Provider)
                                        FactoryMocks.createTableSink(PARTITIONABLE, options)
                                                .getSinkRuntimeProvider(
                                                        new SinkRuntimeProviderContext(false)))
                                .createSink();
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
