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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.enumerator.DefaultReadSessionCreatorFactory;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.query.QuerySpec;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQuerySourceBuilderTest {

    @TempDir Path tempDir;

    @Test
    void buildsABoundedSource() {
        Source<GenericRecord, ?, ?> source = builder().build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void defaultsToTheWholeTableAndBigQuerysOwnStreamCount() {
        BigQuerySourceConfig<GenericRecord> config = TestSources.config();

        assertThat(config.getSelectedFields()).isEmpty();
        assertThat(config.getRowRestriction()).isNull();
        assertThat(config.getSnapshotTime()).isNull();
        assertThat(config.getMaxStreamCount()).isZero();
        assertThat(config.getPreferredMinStreamCount()).isZero();
        // The literal, not the constant: a constant is inlined into this class at compile time, so
        // comparing it against itself would pass for any value — and the reference page states
        // 10000, which nothing else pins.
        assertThat(config.getMaxRecordsPerFetch()).isEqualTo(10_000);
    }

    @Test
    void billsTheReadToTheTablesProjectByDefault() {
        assertThat(TestSources.config().getParentProject()).isEqualTo("p");
    }

    @Test
    void requiresADeserializer() {
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .table(TestSources.TABLE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A deserializer is required");
    }

    @Test
    void rejectsAPreferredMinimumAboveTheMaximum() {
        // BigQuery answers INVALID_ARGUMENT for this ("preferred_min_stream_count must be less than
        // or equal to max_stream_count", measured 2026-08-09); saying so here costs no round trip.
        assertThatThrownBy(() -> builder().maxStreamCount(2).preferredMinStreamCount(8).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preferredMinStreamCount must be at most maxStreamCount");
    }

    @Test
    void acceptsAPreferredMinimumWhenTheMaximumIsLeftToBigQuery() {
        assertThat(TestSources.config(builder -> builder.preferredMinStreamCount(8)))
                .extracting(BigQuerySourceConfig::getPreferredMinStreamCount)
                .isEqualTo(8);
    }

    @Test
    void rejectsNegativeStreamCounts() {
        assertThatThrownBy(() -> builder().maxStreamCount(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStreamCount must not be negative");
        assertThatThrownBy(() -> builder().preferredMinStreamCount(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("preferredMinStreamCount must not be negative");
    }

    @Test
    void rejectsANonPositiveFetchCap() {
        assertThatThrownBy(() -> builder().maxRecordsPerFetch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRecordsPerFetch must be positive");
    }

    @Test
    void handsTheRetryBoundToTheOpenerThatAppliesIt() {
        assertThat(opener(TestSources.config(builder -> builder.retryMaxAttempts(3))))
                .extracting(ReadClientRowStreamOpener::retryMaxAttempts)
                .isEqualTo(3);
    }

    @Test
    void defaultsTheRetryBoundRatherThanLeavingItUnset() {
        // Zero is how gax spells "no bound", so a dropped default would fail nothing — it would
        // quietly restore the twenty-four-hour retry this knob exists to replace. The literal for
        // the reason the fetch cap above uses one: the constant would be inlined into this class
        // and compared against itself.
        assertThat(opener(TestSources.config()))
                .extracting(ReadClientRowStreamOpener::retryMaxAttempts)
                .isEqualTo(25);
    }

    @Test
    void rejectsANonPositiveRetryBound() {
        assertThatThrownBy(() -> builder().retryMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retryMaxAttempts must be positive");
    }

    @Test
    void rejectsABlankOrNullServiceAccountKeyFile() {
        assertThatThrownBy(() -> builder().serviceAccountKeyFile(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("serviceAccountKeyFile must not be blank");
        assertThatThrownBy(() -> builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serviceAccountKeyFile must not be null");
    }

    @Test
    void rejectsServiceAccountCredentialsWithEitherEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                productionTableBuilder()
                                        .serviceAccountKeyFile("key.json")
                                        .emulatorEndpoint("localhost:1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
        assertThatThrownBy(
                        () ->
                                productionQueryBuilder()
                                        .serviceAccountKeyFile("key.json")
                                        .emulatorRestEndpoint("localhost:1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorRestEndpoint(...)");
    }

    @Test
    void credentialPathSurvivesTheJobGraphForEverySourceMode() throws Exception {
        String missingPath = tempDir.resolve("missing-source-secret.json").toString();
        List<BigQueryStorageReadSource<GenericRecord>> sources =
                Arrays.asList(
                        built(productionTableBuilder().serviceAccountKeyFile(missingPath)),
                        built(productionQueryBuilder().serviceAccountKeyFile(missingPath)),
                        built(
                                productionTableBuilder()
                                        .materializeViews()
                                        .serviceAccountKeyFile(missingPath)));

        for (BigQueryStorageReadSource<GenericRecord> source : sources) {
            BigQuerySourceConfig<GenericRecord> config =
                    InstantiationUtil.clone(source).getConfig();

            // The production factory, and what it mints: a builder that stopped wiring the real
            // one would still fail the credential assertion below through whatever it wired.
            assertThat(config.getSessionCreatorFactory())
                    .isInstanceOf(DefaultReadSessionCreatorFactory.class);
            assertThat(config.getSessionCreatorFactory().create())
                    .isInstanceOf(ReadClientSessionCreator.class);
            assertSanitizedCredentialFailure(
                    () ->
                            config.getSessionCreatorFactory()
                                    .create()
                                    .create(CreateReadSessionRequest.getDefaultInstance()),
                    missingPath);
            assertSanitizedCredentialFailure(
                    () -> config.getRowStreamOpener().open("read-stream", 0), missingPath);
            if (config.getQueryRunner() != null) {
                assertSanitizedCredentialFailure(
                        () ->
                                config.getQueryRunner()
                                        .run(new QuerySpec("SELECT 1", "p", null, null)),
                        missingPath);
            }
        }
    }

    @Test
    void anUninitializedRestoreUsesConfiguredCredentialsForEverySourceMode() throws Exception {
        String missingPath = tempDir.resolve("missing-restore-secret.json").toString();
        List<BigQueryStorageReadSource<GenericRecord>> sources =
                Arrays.asList(
                        built(productionTableBuilder().serviceAccountKeyFile(missingPath)),
                        built(productionQueryBuilder().serviceAccountKeyFile(missingPath)),
                        built(
                                productionTableBuilder()
                                        .materializeViews()
                                        .serviceAccountKeyFile(missingPath)));
        BigQueryReadEnumeratorState beforePlanning =
                new BigQueryReadEnumeratorState(false, null, null, Collections.emptyList());

        for (BigQueryStorageReadSource<GenericRecord> source : sources) {
            FakeSplitEnumeratorContext<ReadStreamSplit> context =
                    new FakeSplitEnumeratorContext<>(1);
            try (SplitEnumerator<ReadStreamSplit, BigQueryReadEnumeratorState> enumerator =
                    InstantiationUtil.clone(source).restoreEnumerator(context, beforePlanning)) {
                enumerator.start();
                assertThatThrownBy(context::runAsyncCalls)
                        .hasRootCauseInstanceOf(IOException.class)
                        .hasRootCauseMessage(
                                "Failed to load the configured BigQuery service-account key file.")
                        .hasMessageNotContaining(missingPath);
            }
        }
    }

    @Test
    void anInitializedRestoreDoesNotReopenPlanningClients() throws Exception {
        String missingPath = tempDir.resolve("missing-restored-secret.json").toString();
        BigQueryStorageReadSource<GenericRecord> source =
                InstantiationUtil.clone(
                        built(productionQueryBuilder().serviceAccountKeyFile(missingPath)));
        BigQueryReadEnumeratorState initialized =
                new BigQueryReadEnumeratorState(
                        true, "projects/p/locations/us/sessions/s", null, Collections.emptyList());
        FakeSplitEnumeratorContext<ReadStreamSplit> context = new FakeSplitEnumeratorContext<>(1);

        try (SplitEnumerator<ReadStreamSplit, BigQueryReadEnumeratorState> enumerator =
                source.restoreEnumerator(context, initialized)) {
            enumerator.start();
            context.runAsyncCalls();

            assertThat(enumerator.snapshotState(1L).isInitialized()).isTrue();
        }
    }

    @Test
    void takesTheSelectedFieldsAsACollectionToo() {
        assertThat(
                        TestSources.config(
                                        builder ->
                                                builder.selectedFields(Arrays.asList("id", "name")))
                                .getSelectedFields())
                .containsExactly("id", "name");
    }

    @Test
    void rejectsABlankOrRepeatedSelectedField() {
        assertThatThrownBy(() -> builder().selectedFields("id", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
        assertThatThrownBy(() -> builder().selectedFields("id", "id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("named twice");
    }

    @Test
    void rejectsABlankRowRestrictionOrParentProject() {
        assertThatThrownBy(() -> builder().rowRestriction(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowRestriction must not be blank");
        assertThatThrownBy(() -> builder().parentProject(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parentProject must not be blank");
    }

    @Test
    void rejectsAMalformedEmulatorEndpointWhereItIsTyped() {
        // The message names the setter that was called rather than a fixed one: BigQuery is the
        // connector with two endpoint setters, and a user who mistypes the REST one must not be
        // sent to the gRPC one (#895).
        assertThatThrownBy(() -> builder().emulatorEndpoint("not-a-host-port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'not-a-host-port'");
        assertThatThrownBy(() -> builder().emulatorRestEndpoint("not-a-host-port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorRestEndpoint must be host:port, was 'not-a-host-port'");
        assertThatThrownBy(() -> builder().emulatorRestEndpoint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorRestEndpoint must not be null");
    }

    @Test
    void requiresATableOrAQueryAndRefusesBoth() {
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .deserializer(deserializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("set table(...) or query(...)");
        assertThatThrownBy(() -> builder().query("SELECT 1").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("alternatives");
    }

    @Test
    void buildsAQuerySourceThatCarriesItsQueryAndNoTable() {
        BigQuerySourceConfig<GenericRecord> config =
                TestSources.queryConfig(
                        builder -> builder.queryLocation("US").queryResultDataset("scratch"));

        assertThat(config.getTable()).isNull();
        assertThat(config.getQuery()).isEqualTo(TestSources.QUERY);
        assertThat(config.getQueryLocation()).isEqualTo("US");
        assertThat(config.getQueryResultDataset()).isEqualTo("scratch");
        assertThat(config.getQueryRunner()).isNotNull();
        assertThat(config.describeInput()).isEqualTo("the result of the configured query");
    }

    @Test
    void shipsAQuerySourceThroughTheJobGraph() throws Exception {
        // Everything a query source adds travels with it: the query, where its result goes, and the
        // runner. A non-serializable field here fails at job submission and nowhere earlier, which
        // is the one job-level risk the enumerator's own tests cannot reach.
        BigQueryStorageReadSource<GenericRecord> source =
                (BigQueryStorageReadSource<GenericRecord>)
                        BigQuerySource.<GenericRecord>builder()
                                .query("SELECT 1 AS id")
                                .parentProject("p")
                                .queryResultDataset("scratch")
                                .deserializer(deserializer())
                                .emulatorEndpoint("localhost:1")
                                .emulatorRestEndpoint("localhost:1")
                                .build();

        BigQueryStorageReadSource<GenericRecord> copy = InstantiationUtil.clone(source);

        assertThat(copy.getConfig().getQuery()).isEqualTo("SELECT 1 AS id");
        assertThat(copy.getConfig().getQueryResultDataset()).isEqualTo("scratch");
        assertThat(copy.getConfig().getQueryRunner()).isNotNull();
    }

    @Test
    void landsTheResultInBigQuerysAnonymousDatasetUnlessADatasetIsNamed() {
        // Unset is what makes the default path the one with nothing to create, nothing to expire
        // and nothing to delete.
        assertThat(TestSources.queryConfig().getQueryResultDataset()).isNull();
    }

    @Test
    void buildsATableSourceThatRunsNoQuery() {
        BigQuerySourceConfig<GenericRecord> config = TestSources.config();

        assertThat(config.getQuery()).isNull();
        assertThat(config.getQueryRunner()).isNull();
        assertThat(config.describeInput()).isEqualTo("table p.d.t");
    }

    @Test
    void requiresAParentProjectBesideAQuery() {
        // A table source takes the table's project; a query names no table, so nothing else says
        // which project the job is submitted to and billed to.
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .query("SELECT 1")
                                        .deserializer(deserializer())
                                        .emulatorEndpoint("localhost:1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("query(...) requires parentProject(...)");
    }

    @Test
    void rejectsTheQueryKnobsOnATableSource() {
        assertThatThrownBy(() -> builder().queryLocation("US").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "queryLocation(...) applies to query(...) or materializeViews() only");
        assertThatThrownBy(() -> builder().queryResultDataset("scratch").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "queryResultDataset(...) applies to query(...) or materializeViews() only");
        // Silently ignoring it would leave a test pointing half its traffic at the emulator and
        // half at BigQuery, which is the shape that reads as a flake rather than as
        // misconfiguration.
        assertThatThrownBy(() -> builder().emulatorRestEndpoint("localhost:1").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "emulatorRestEndpoint(...) applies to query(...) or materializeViews() only");
    }

    @Test
    void rejectsTheReuseWindowOnATableSourceAndWhereItIsTyped() {
        assertThatThrownBy(() -> builder().reuseQueryResultWithin(Duration.ofHours(1)).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "reuseQueryResultWithin(...) applies to query(...) or materializeViews()"
                                + " only");
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .reuseQueryResultWithin(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .reuseQueryResultWithin(Duration.ofHours(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
        // Both landing places expire after about a day, so a longer window has nothing to reuse
        // — every older adoption would find its table gone and fall back to running the query —
        // rejected where the value was typed rather than shipped as a knob that cannot work.
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .reuseQueryResultWithin(Duration.ofHours(24).plusMillis(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 24 hours");
    }

    @Test
    void theReuseWindowRequiresAQueryLocation() {
        // BigQuery scopes a job to (project, location, id); a look-up naming no location sees
        // only the US multi-region, so without one the previous attempt's job would never be
        // found — measured against a regional dataset, and rejected where the knob is typed.
        assertThatThrownBy(
                        () ->
                                TestSources.queryConfig(
                                        b -> b.reuseQueryResultWithin(Duration.ofHours(1))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires queryLocation(...)");
    }

    @Test
    void acceptsTheReuseWindowUpToADayOnAQuerySource() {
        assertThat(
                        TestSources.queryConfig(
                                        b ->
                                                b.queryLocation("asia-northeast1")
                                                        .reuseQueryResultWithin(
                                                                Duration.ofHours(24)))
                                .getReuseQueryResultWithin())
                .isEqualTo(Duration.ofHours(24));
        // Off unless asked for: the default keeps today's random id and runs the query per plan.
        assertThat(TestSources.queryConfig().getReuseQueryResultWithin()).isNull();
    }

    @Test
    void materializeViewsIsOffUnlessAskedFor() {
        assertThat(TestSources.config().isMaterializeViewsEnabled()).isFalse();
        assertThat(TestSources.config(b -> b.materializeViews()).isMaterializeViewsEnabled())
                .isTrue();
        // The runner is what makes the metadata call, so a source without the opt-in must not even
        // hold one — that absence is the "no REST call on the read path" property.
        assertThat(TestSources.config().getQueryRunner()).isNull();
        assertThat(TestSources.config(b -> b.materializeViews()).getQueryRunner()).isNotNull();
    }

    @Test
    void rejectsMaterializeViewsBesideAQuery() {
        assertThatThrownBy(() -> TestSources.queryConfig(b -> b.materializeViews()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("materializeViews() applies to table(...) only");
    }

    @Test
    void letsTheQueryKnobsThroughWhenViewsAreMaterialized() {
        // They describe the query job, and materializeViews() is the other way to get one.
        BigQuerySourceConfig<GenericRecord> config =
                TestSources.config(
                        b ->
                                b.materializeViews()
                                        .queryLocation("US")
                                        .queryResultDataset("scratch")
                                        .emulatorRestEndpoint("localhost:1"));

        assertThat(config.getQueryLocation()).isEqualTo("US");
        assertThat(config.getQueryResultDataset()).isEqualTo("scratch");
    }

    @Test
    void rejectsASnapshotTimeBesideMaterializeViews() {
        // A view's result table is created now, so time travel over it has nothing to reach — and
        // the read would fail at session creation, far from where the value was typed.
        assertThatThrownBy(
                        () ->
                                TestSources.config(
                                        b ->
                                                b.materializeViews()
                                                        .snapshotTime(
                                                                Instant.parse(
                                                                        "2026-08-01T00:00:00Z"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("do not go together");
    }

    @Test
    void rejectsASnapshotTimeBesideAQuery() {
        // The result table is created by the query, so there is no earlier version of it to read.
        // Ignoring the knob would read the current result and look like it had been honoured.
        assertThatThrownBy(
                        () ->
                                TestSources.queryConfig(
                                        b -> b.snapshotTime(Instant.parse("2026-08-01T00:00:00Z"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FOR SYSTEM_TIME AS OF");
    }

    @Test
    void rejectsABlankQueryOrItsCompanionsWhereTheyAreTyped() {
        assertThatThrownBy(() -> builder().query(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query must not be blank");
        assertThatThrownBy(() -> builder().queryLocation(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryLocation must not be blank");
        assertThatThrownBy(() -> builder().queryResultDataset(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryResultDataset must not be blank");
    }

    private static BigQuerySourceBuilder<GenericRecord> builder() {
        return BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("p", "d", "t"))
                .deserializer(deserializer())
                // The builder creates this source's real clients; the endpoint is never connected
                // to, but without it a machine with application-default credentials passes where
                // CI fails.
                .emulatorEndpoint("localhost:1");
    }

    private static BigQuerySourceBuilder<GenericRecord> productionTableBuilder() {
        return BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("p", "d", "t"))
                .deserializer(deserializer());
    }

    private static BigQuerySourceBuilder<GenericRecord> productionQueryBuilder() {
        return BigQuerySource.<GenericRecord>builder()
                .query("SELECT 1")
                .parentProject("p")
                .deserializer(deserializer());
    }

    @SuppressWarnings("unchecked")
    private static BigQueryStorageReadSource<GenericRecord> built(
            BigQuerySourceBuilder<GenericRecord> builder) {
        return (BigQueryStorageReadSource<GenericRecord>) builder.build();
    }

    private static void assertSanitizedCredentialFailure(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable call, String path) {
        assertThatThrownBy(call)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause()
                .hasMessageNotContaining(path);
    }

    private static BigQueryRowDeserializationSchema<GenericRecord> deserializer() {
        return BigQueryRowDeserializationSchema.genericRecord(TestRows.SCHEMA_JSON);
    }

    private static ReadClientRowStreamOpener opener(BigQuerySourceConfig<?> config) {
        return (ReadClientRowStreamOpener) config.getRowStreamOpener();
    }
}
