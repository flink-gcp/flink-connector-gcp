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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.SchemaUpdateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
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

/** Tests for {@link BigQueryDynamicSink}. */
class BigQueryDynamicSinkTest {

    private static final DataType ROW =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("amount", DataTypes.BIGINT()));

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    /**
     * The constructor's arguments, so a test can vary one by name.
     *
     * <p>The sink takes eleven positional arguments, eight of them {@code null} in the default
     * case, and the identity test below has to build one variation per argument — written out, the
     * argument lists were longer than the assertions and a new field meant editing every one of
     * them.
     */
    private static final class Args {
        private DataType physicalDataType = ROW;
        private TableDestination destination = DESTINATION;
        private RowDataSchemaOptions schemaOptions = RowDataSchemaOptions.defaults();
        private CreateDisposition createDisposition;
        private TableCreateOptions tableCreateOptions;
        private String location;
        private SchemaUpdateOptions schemaUpdateOptions;
        private DefaultStreamOptions defaultStreamOptions;
        private String emulatorEndpoint;
        private String emulatorRestEndpoint;
        private Integer parallelism;

        private BigQueryDynamicSink build() {
            return new BigQueryDynamicSink(
                    physicalDataType,
                    destination,
                    schemaOptions,
                    createDisposition,
                    tableCreateOptions,
                    location,
                    schemaUpdateOptions,
                    defaultStreamOptions,
                    emulatorEndpoint,
                    emulatorRestEndpoint,
                    parallelism);
        }
    }

    private static BigQueryDynamicSink sink() {
        return new Args().build();
    }

    private static BigQueryDynamicSink sinkWith(Consumer<Args> vary) {
        Args args = new Args();
        vary.accept(args);
        return args.build();
    }

    @Test
    void isInsertOnlyWhateverThePlannerAsksFor() {
        assertThat(sink().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(sink().getChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void describesItselfByName() {
        assertThat(sink().asSummaryString()).isEqualTo("BigQuery table sink");
    }

    @Test
    void aCopyEqualsTheOriginal() {
        DynamicTableSink copy = sink().copy();
        assertThat(copy).isEqualTo(sink()).hasSameHashCodeAs(sink());
        assertThat(copy).isNotSameAs(sink());
    }

    /** One variation per field of the sink, keyed by the field it varies. */
    private static Map<String, BigQueryDynamicSink> variations() {
        Map<String, BigQueryDynamicSink> varied = new LinkedHashMap<>();
        varied.put(
                "physicalDataType",
                sinkWith(
                        a ->
                                a.physicalDataType =
                                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()))));
        varied.put(
                "destination",
                sinkWith(
                        a ->
                                a.destination =
                                        TableDestination.of(
                                                "my-project", "my_dataset", "other_table")));
        varied.put(
                "schemaOptions",
                sinkWith(
                        a ->
                                a.schemaOptions =
                                        RowDataSchemaOptions.builder()
                                                .jsonFieldPaths(Collections.singletonList("id"))
                                                .build()));
        varied.put(
                "createDisposition",
                sinkWith(a -> a.createDisposition = CreateDisposition.CREATE_NEVER));
        varied.put(
                "tableCreateOptions",
                sinkWith(
                        a ->
                                a.tableCreateOptions =
                                        TableCreateOptions.builder()
                                                .timePartitioning(
                                                        TableCreateOptions.TimePartitioningType.DAY)
                                                .build()));
        varied.put("location", sinkWith(a -> a.location = "US"));
        varied.put(
                "schemaUpdateOptions",
                sinkWith(
                        a ->
                                a.schemaUpdateOptions =
                                        SchemaUpdateOptions.builder().allowNewFields().build()));
        varied.put(
                "defaultStreamOptions",
                sinkWith(
                        a ->
                                a.defaultStreamOptions =
                                        DefaultStreamOptions.builder()
                                                .maxInflightRequests(5)
                                                .build()));
        varied.put("emulatorEndpoint", sinkWith(a -> a.emulatorEndpoint = "localhost:9060"));
        varied.put(
                "emulatorRestEndpoint", sinkWith(a -> a.emulatorRestEndpoint = "localhost:9050"));
        varied.put("parallelism", sinkWith(a -> a.parallelism = 3));
        return varied;
    }

    @Test
    void everyFieldOfTheSinkIsPartOfItsIdentity() {
        BigQueryDynamicSink base = sink();
        variations()
                .forEach(
                        (field, varied) ->
                                assertThat(varied).as("varying %s", field).isNotEqualTo(base));
    }

    @Test
    void everyFieldOfTheSinkIsActuallyVaried() {
        // The half the assertions above cannot make: a field added to the sink and forgotten here
        // reads exactly like a field that is covered, and a value dropped from equals() would then
        // go unnoticed. Reflection is what makes the list exhaustive rather than remembered.
        List<String> declared = new ArrayList<>();
        for (Field field : BigQueryDynamicSink.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                declared.add(field.getName());
            }
        }

        assertThat(variations().keySet()).containsExactlyInAnyOrderElementsOf(declared);
    }
}
