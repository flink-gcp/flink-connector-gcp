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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQueryDynamicSink}. */
class BigQueryDynamicSinkTest {

    private static final DataType ROW =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("amount", DataTypes.BIGINT()));

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    /**
     * The sink's three required values, so a test can vary one of the other twelve by name.
     *
     * <p>The production builder is the holder: the identity test below builds one variation per
     * field, and a private copy of the same fifteen fields would have to be kept in step with it
     * for no gain.
     */
    private static BigQueryDynamicSink.Builder base() {
        return BigQueryDynamicSink.builder()
                .physicalDataType(ROW)
                .destination(DESTINATION)
                .schemaOptions(RowDataSchemaOptions.defaults())
                .primaryKeyIndexes(new int[] {0});
    }

    private static BigQueryDynamicSink sink() {
        return base().build();
    }

    private static BigQueryDynamicSink sinkWith(Consumer<BigQueryDynamicSink.Builder> vary) {
        BigQueryDynamicSink.Builder builder = base();
        vary.accept(builder);
        return builder.build();
    }

    @Test
    void isInsertOnlyWhateverThePlannerAsksFor() {
        assertThat(sink().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink().getChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void cdcAcceptsAnUpsertChangelogButKeepsAppendPlansInsertOnly() {
        BigQueryDynamicSink cdc = sinkWith(builder -> builder.cdcEnabled(true));

        assertThat(cdc.getChangelogMode(ChangelogMode.all()).getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
        assertThat(cdc.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void exposesTheTwoAlternativeCdcSequenceMetadataColumns() {
        Map<String, DataType> expected = new LinkedHashMap<>();
        expected.put("change-sequence-number", DataTypes.STRING().nullable());
        expected.put(
                "debezium-source-properties",
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .nullable());

        assertThat(((SupportsWritingMetadata) sink()).listWritableMetadata())
                .containsExactlyEntriesOf(expected);
    }

    @Test
    void writableMetadataRequiresCdcAndExactlyOneSequenceSource() {
        BigQueryDynamicSink ordinary = sink();
        assertThatThrownBy(
                        () ->
                                ordinary.applyWritableMetadata(
                                        Collections.singletonList("change-sequence-number"), ROW))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("available only when 'sink.cdc.enabled' = 'true'");

        BigQueryDynamicSink cdc = sinkWith(builder -> builder.cdcEnabled(true));
        assertThatThrownBy(
                        () ->
                                cdc.applyWritableMetadata(
                                        List.of(
                                                "change-sequence-number",
                                                "debezium-source-properties"),
                                        ROW))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Select exactly one BigQuery CDC sequence source");
    }

    @Test
    void describesItselfByName() {
        assertThat(sink().asSummaryString()).isEqualTo("BigQuery table sink");
    }

    /** One value per field of the sink, keyed by the field it sets. */
    private static Map<String, Consumer<BigQueryDynamicSink.Builder>> variations() {
        Map<String, Consumer<BigQueryDynamicSink.Builder>> varied = new LinkedHashMap<>();
        varied.put(
                "physicalDataType",
                a -> a.physicalDataType(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()))));
        varied.put(
                "destination",
                a -> a.destination(TableDestination.of("my-project", "my_dataset", "other_table")));
        varied.put(
                "schemaOptions",
                a ->
                        a.schemaOptions(
                                RowDataSchemaOptions.builder()
                                        .jsonFieldPaths(Collections.singletonList("id"))
                                        .build()));
        varied.put("cdcEnabled", a -> a.cdcEnabled(true));
        varied.put("primaryKeyIndexes", a -> a.primaryKeyIndexes(new int[] {1}));
        varied.put("writeMethod", a -> a.writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE));
        varied.put("createDisposition", a -> a.createDisposition(CreateDisposition.CREATE_NEVER));
        varied.put(
                "tableCreateOptions",
                a ->
                        a.tableCreateOptions(
                                TableCreateOptions.builder()
                                        .timePartitioning(
                                                TableCreateOptions.TimePartitioningType.DAY)
                                        .build()));
        varied.put("location", a -> a.location("US"));
        varied.put(
                "schemaUpdateOptions",
                a -> a.schemaUpdateOptions(SchemaUpdateOptions.builder().allowNewFields().build()));
        varied.put(
                "defaultStreamOptions",
                a ->
                        a.defaultStreamOptions(
                                DefaultStreamOptions.builder().maxInflightRequests(5).build()));
        varied.put(
                "bufferedStreamOptions",
                a ->
                        a.bufferedStreamOptions(
                                BufferedStreamOptions.builder().retryMaxAttempts(7).build()));
        varied.put(
                "fileLoadsOptions",
                a ->
                        a.fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://bucket/prefix")
                                        .build()));
        varied.put(
                "serviceAccountKeyFile",
                a -> a.serviceAccountKeyFile("/var/run/secrets/bigquery-key.json"));
        varied.put("emulatorEndpoint", a -> a.emulatorEndpoint("localhost:9060"));
        varied.put("emulatorRestEndpoint", a -> a.emulatorRestEndpoint("localhost:9050"));
        varied.put("parallelism", a -> a.parallelism(3));
        varied.put(
                "metadataKeys",
                a ->
                        a.cdcEnabled(true)
                                .metadataKeys(Collections.singletonList("change-sequence-number")));
        return varied;
    }

    /** A sink with every field set, built by applying all of {@link #variations()} at once. */
    private static BigQueryDynamicSink fullySpecified() {
        BigQueryDynamicSink.Builder builder = base();
        variations().values().forEach(vary -> vary.accept(builder));
        return builder.build();
    }

    @Test
    void aCopyOfAFullySpecifiedSinkEqualsIt() {
        // Fully specified, not the default one: copy() is a chain of fifteen builder calls, and a
        // dropped call reproduces whatever the default already was — copying a sink whose optional
        // fields are all null cannot tell the two apart. Measured: a copy() that lost writeMethod
        // survived that version of this test.
        DynamicTableSink copy = fullySpecified().copy();
        assertThat(copy).isEqualTo(fullySpecified()).hasSameHashCodeAs(fullySpecified());
        assertThat(copy).isNotSameAs(fullySpecified());
    }

    @Test
    void everyFieldOfTheSinkIsPartOfItsIdentity() {
        BigQueryDynamicSink base = sink();
        variations()
                .forEach(
                        (field, vary) ->
                                assertThat(sinkWith(vary))
                                        .as("varying %s", field)
                                        .isNotEqualTo(base));
    }

    @Test
    void everyVariationVariesTheFieldItIsKeyedBy() {
        // The map's keys are labels, and nothing else ties one to the field it names. That matters
        // more since fullySpecified() applied them all at once: two entries touching one field
        // would leave a third at its default, and a copy() that dropped *that* call would survive
        // the test above. So each entry is checked to change its own field and no other.
        variations()
                .forEach(
                        (field, vary) -> {
                            BigQueryDynamicSink base =
                                    field.equals("metadataKeys")
                                            ? sinkWith(builder -> builder.cdcEnabled(true))
                                            : sink();
                            BigQueryDynamicSink varied = sinkWith(vary);
                            for (Field declared : BigQueryDynamicSink.class.getDeclaredFields()) {
                                if (Modifier.isStatic(declared.getModifiers())) {
                                    continue;
                                }
                                declared.setAccessible(true);
                                try {
                                    assertThat(declared.get(varied))
                                            .as("%s varied %s", field, declared.getName())
                                            .satisfies(
                                                    value -> {
                                                        if (declared.getName().equals(field)) {
                                                            assertThat(value)
                                                                    .isNotEqualTo(
                                                                            declared.get(base));
                                                        } else {
                                                            assertThat(value)
                                                                    .isEqualTo(declared.get(base));
                                                        }
                                                    });
                                } catch (IllegalAccessException e) {
                                    throw new AssertionError(e);
                                }
                            }
                        });
    }

    @Test
    void everyFieldOfTheSinkIsActuallyVaried() {
        // The half the assertions above cannot make: a field added to the sink and forgotten here
        // reads exactly like a field that is covered, and a value dropped from equals() or from
        // copy() would then go unnoticed. Reflection is what makes the list exhaustive rather than
        // remembered.
        List<String> declared = new ArrayList<>();
        for (Field field : BigQueryDynamicSink.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                declared.add(field.getName());
            }
        }

        assertThat(variations().keySet()).containsExactlyInAnyOrderElementsOf(declared);
    }
}
