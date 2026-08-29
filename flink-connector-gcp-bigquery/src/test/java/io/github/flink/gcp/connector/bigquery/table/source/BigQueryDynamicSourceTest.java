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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The source's identity contract.
 *
 * <p>The planner deduplicates scans through {@code equals}/{@code hashCode}, so a field the
 * comparison misses is a field two differently-configured reads can silently share one scan over.
 * The factory test covers the all-equal side through {@code copy()}, which shares every reference
 * and therefore runs no field comparison at all; these cases are the inequality arms, one field at
 * a time.
 */
class BigQueryDynamicSourceTest {

    @Test
    void equalSourcesAgreeOnEqualsAndHashCode() {
        assertThat(source()).isEqualTo(source());
        assertThat(source().hashCode()).isEqualTo(source().hashCode());
    }

    @Test
    void isNeverEqualToNullOrAnotherType() {
        assertThat(source()).isNotEqualTo(null).isNotEqualTo("BigQuery table source");
    }

    @Test
    void comparesEveryBuilderField() {
        BigQueryDynamicSource base = source();

        variations()
                .forEach(
                        (field, vary) -> assertThat(sourceWith(vary)).as(field).isNotEqualTo(base));
    }

    @Test
    void comparesTwoQuerySourcesByTheirQueries() {
        // The table-against-query variation changes two fields at once, because exactly one of the
        // two may be set; this is the query term on its own.
        BigQueryDynamicSource first =
                sourceWith(
                        args -> {
                            args.table = null;
                            args.query = "SELECT 1";
                        });
        BigQueryDynamicSource second =
                sourceWith(
                        args -> {
                            args.table = null;
                            args.query = "SELECT 2";
                        });

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void comparesThePhysicalTypeApartFromTheProducedType() {
        // A source that was not copied derives producedDataType from physicalDataType, so a
        // builder variation cannot separate the two terms; applying the same projection and
        // produced type to both sides pins the physical half on its own.
        DataType produced = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));
        BigQueryDynamicSource base = source();
        base.applyProjection(new int[][] {{0}}, produced);
        BigQueryDynamicSource otherPhysical =
                sourceWith(
                        args ->
                                args.physicalDataType =
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                                DataTypes.FIELD("name", DataTypes.INT())));
        otherPhysical.applyProjection(new int[][] {{0}}, produced);

        assertThat(otherPhysical).as("physicalRowType").isNotEqualTo(base);
    }

    @Test
    void comparesTheAppliedProjectionAndProducedTypeIndependently() {
        DataType produced = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));
        BigQueryDynamicSource first = source();
        first.applyProjection(new int[][] {{0}}, produced);
        BigQueryDynamicSource sameAgain = source();
        sameAgain.applyProjection(new int[][] {{0}}, produced);
        BigQueryDynamicSource otherColumn = source();
        otherColumn.applyProjection(new int[][] {{1}}, produced);
        BigQueryDynamicSource otherProduced = source();
        otherProduced.applyProjection(
                new int[][] {{0}}, DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())));

        assertThat(first).isEqualTo(sameAgain);
        assertThat(first.hashCode()).isEqualTo(sameAgain.hashCode());
        // One term at a time: another column under the same produced type, and the same column
        // read as another produced type.
        assertThat(otherColumn).as("projectedFields").isNotEqualTo(first);
        assertThat(otherProduced).as("producedDataType").isNotEqualTo(first);
    }

    @Test
    void aCopyOfAFullySpecifiedSourceRepeatsEveryFieldOfIt() throws Exception {
        // The guard ADR-0032 records the sink needing when it took a builder: a dropped positional
        // argument does not compile, a dropped builder call does. Reflection rather than a list of
        // getters, because the half worth catching is a field added later and forgotten in copy() —
        // which the option-driven copies in the factory test catch only if some DDL there happens
        // to set it.
        BigQueryDynamicSource source = projected(fullySpecified());
        BigQueryDynamicSource copy = (BigQueryDynamicSource) source.copy();

        for (Field declared : BigQueryDynamicSource.class.getDeclaredFields()) {
            if (Modifier.isStatic(declared.getModifiers())) {
                continue;
            }
            declared.setAccessible(true);
            assertThat(declared.get(copy))
                    .as("copy() dropped %s", declared.getName())
                    .isEqualTo(declared.get(source));
        }
    }

    @Test
    void theFullySpecifiedSourceLeavesNoFieldAtItsDefault() throws Exception {
        // What makes the guard above cover a field added *later*, and the half it does not have on
        // its own: fullySpecified() takes its values from the hand-written variations() map, so a
        // new field nobody adds there sits at its default on both sides of the copy and compares
        // equal however copy() treats it. This is the assertion that fails when that happens.
        BigQueryDynamicSource base = source();
        BigQueryDynamicSource full = projected(fullySpecified());

        for (Field declared : BigQueryDynamicSource.class.getDeclaredFields()) {
            if (Modifier.isStatic(declared.getModifiers())) {
                continue;
            }
            declared.setAccessible(true);
            assertThat(declared.get(full))
                    .as("variations() leaves %s at its default", declared.getName())
                    .isNotEqualTo(declared.get(base));
        }
    }

    /** A source with every value set, by applying all of {@link #variations()} to one base. */
    private static BigQueryDynamicSource fullySpecified() {
        Args args = new Args();
        variations().values().forEach(vary -> vary.accept(args));
        return args.build();
    }

    /** The same source with the two planner-applied fields set, which no builder value reaches. */
    private static BigQueryDynamicSource projected(BigQueryDynamicSource source) {
        source.applyProjection(
                new int[][] {{0}}, DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())));
        return source;
    }

    /**
     * One variation per builder field, each differing from {@link #source()} in that field.
     *
     * <p>Two entries are inherently wider than one field: the table-against-query swap (exactly one
     * of the two may be set — {@link #comparesTwoQuerySourcesByTheirQueries} covers the query term
     * on its own) and the physical type, which an uncopied source also takes as its produced type
     * ({@link #comparesThePhysicalTypeApartFromTheProducedType} isolates it).
     */
    private static Map<String, Consumer<Args>> variations() {
        Map<String, Consumer<Args>> varied = new LinkedHashMap<>();
        varied.put(
                "physicalRowType and producedDataType",
                args ->
                        args.physicalDataType =
                                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())));
        varied.put("table", args -> args.table = TableDestination.of("p", "d", "t2"));
        varied.put(
                "table against query",
                args -> {
                    args.table = null;
                    args.query = "SELECT 1";
                });
        varied.put("parentProject", args -> args.parentProject = "p2");
        varied.put("materializeViews", args -> args.materializeViews = true);
        varied.put("queryLocation", args -> args.queryLocation = "US");
        varied.put("queryResultDataset", args -> args.queryResultDataset = "scratch");
        varied.put(
                "reuseQueryResultWithin",
                args -> args.reuseQueryResultWithin = Duration.ofMinutes(10));
        varied.put("rowRestriction", args -> args.rowRestriction = "id > 0");
        varied.put("snapshotTime", args -> args.snapshotTime = Instant.EPOCH);
        varied.put("maxStreamCount", args -> args.maxStreamCount = 7);
        varied.put("preferredMinStreamCount", args -> args.preferredMinStreamCount = 3);
        varied.put("maxRecordsPerFetch", args -> args.maxRecordsPerFetch = 200);
        varied.put("maxBytesPerFetch", args -> args.maxBytesPerFetch = 4096L);
        varied.put("retryMaxAttempts", args -> args.retryMaxAttempts = 9);
        varied.put("serviceAccountKeyFile", args -> args.serviceAccountKeyFile = "/key.json");
        varied.put("emulatorEndpoint", args -> args.emulatorEndpoint = "localhost:1");
        varied.put("emulatorRestEndpoint", args -> args.emulatorRestEndpoint = "localhost:2");
        varied.put("parallelism", args -> args.parallelism = 2);
        return varied;
    }

    private static BigQueryDynamicSource source() {
        return new Args().build();
    }

    private static BigQueryDynamicSource sourceWith(Consumer<Args> vary) {
        Args args = new Args();
        vary.accept(args);
        return args.build();
    }

    /** The base source's builder values, one field away at a time. */
    private static final class Args {

        DataType physicalDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()));
        @Nullable TableDestination table = TableDestination.of("p", "d", "t");
        @Nullable String query;
        String parentProject = "p";
        boolean materializeViews;
        @Nullable String queryLocation;
        @Nullable String queryResultDataset;
        @Nullable Duration reuseQueryResultWithin;
        @Nullable String rowRestriction;
        @Nullable Instant snapshotTime;
        @Nullable Integer maxStreamCount;
        @Nullable Integer preferredMinStreamCount;
        @Nullable Integer maxRecordsPerFetch;
        @Nullable Long maxBytesPerFetch;
        @Nullable Integer retryMaxAttempts;
        @Nullable String serviceAccountKeyFile;
        @Nullable String emulatorEndpoint;
        @Nullable String emulatorRestEndpoint;
        @Nullable Integer parallelism;

        BigQueryDynamicSource build() {
            return BigQueryDynamicSource.builder()
                    .physicalDataType(physicalDataType)
                    .table(table)
                    .query(query)
                    .parentProject(parentProject)
                    .materializeViews(materializeViews)
                    .queryLocation(queryLocation)
                    .queryResultDataset(queryResultDataset)
                    .reuseQueryResultWithin(reuseQueryResultWithin)
                    .rowRestriction(rowRestriction)
                    .snapshotTime(snapshotTime)
                    .maxStreamCount(maxStreamCount)
                    .preferredMinStreamCount(preferredMinStreamCount)
                    .maxRecordsPerFetch(maxRecordsPerFetch)
                    .maxBytesPerFetch(maxBytesPerFetch)
                    .retryMaxAttempts(retryMaxAttempts)
                    .serviceAccountKeyFile(serviceAccountKeyFile)
                    .emulatorEndpoint(emulatorEndpoint)
                    .emulatorRestEndpoint(emulatorRestEndpoint)
                    .parallelism(parallelism)
                    .build();
        }
    }
}
