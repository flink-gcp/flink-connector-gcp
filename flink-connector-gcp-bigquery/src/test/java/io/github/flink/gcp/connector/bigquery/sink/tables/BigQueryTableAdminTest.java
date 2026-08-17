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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.http.HttpTransportOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.StubBigQuery;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQueryTableAdmin}. */
class BigQueryTableAdminTest {

    @TempDir Path tempDir;

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("event_ts")
                                    .setType(TableFieldSchema.Type.TIMESTAMP)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();

    @Test
    void configuredCredentialsAreLoadedWhenTheRestClientIsFirstUsed() {
        String missingPath = tempDir.resolve("missing-table-admin-secret.json").toString();
        BigQueryTableAdmin admin = new BigQueryTableAdmin(missingPath, null);

        assertThatThrownBy(() -> admin.getSchema(DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured BigQuery service-account key file.")
                .hasNoCause()
                .hasMessageNotContaining(missingPath);
    }

    @Test
    void buildsPlainTableByDefault() {
        TableInfo tableInfo =
                BigQueryTableAdmin.buildTableInfo(
                        DESTINATION, SCHEMA, TableCreateOptions.defaults());

        assertThat(tableInfo.getTableId().getProject()).isEqualTo("p");
        assertThat(tableInfo.getTableId().getDataset()).isEqualTo("d");
        assertThat(tableInfo.getTableId().getTable()).isEqualTo("t");
        StandardTableDefinition definition = tableInfo.getDefinition();
        assertThat(definition.getSchema().getFields().get("event_ts")).isNotNull();
        assertThat(definition.getTimePartitioning()).isNull();
        assertThat(definition.getClustering()).isNull();
    }

    @Test
    void appliesPartitioningAndClustering() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "event_ts")
                        .timePartitioningExpiration(Duration.ofDays(90))
                        .clusteredFields(Arrays.asList("event_ts"))
                        .build();

        TableInfo tableInfo = BigQueryTableAdmin.buildTableInfo(DESTINATION, SCHEMA, options);

        StandardTableDefinition definition = tableInfo.getDefinition();
        TimePartitioning partitioning = definition.getTimePartitioning();
        assertThat(partitioning.getType()).isEqualTo(TimePartitioning.Type.DAY);
        assertThat(partitioning.getField()).isEqualTo("event_ts");
        assertThat(partitioning.getExpirationMs()).isEqualTo(Duration.ofDays(90).toMillis());
        assertThat(definition.getClustering().getFields()).containsExactly("event_ts");
    }

    @Test
    void appliesIngestionTimePartitioning() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.MONTH)
                        .build();

        TableInfo tableInfo = BigQueryTableAdmin.buildTableInfo(DESTINATION, SCHEMA, options);

        StandardTableDefinition definition = tableInfo.getDefinition();
        assertThat(definition.getTimePartitioning().getType())
                .isEqualTo(TimePartitioning.Type.MONTH);
        assertThat(definition.getTimePartitioning().getField()).isNull();
    }

    @Test
    void appliesPrimaryKeyThroughTheTablesApiRequest() {
        TableInfo tableInfo =
                BigQueryTableAdmin.buildTableInfo(
                        DESTINATION,
                        SCHEMA,
                        TableCreateOptions.defaults(),
                        CdcTableOptions.builder()
                                .primaryKeyColumns(Arrays.asList("event_ts", "tenant"))
                                .build());

        assertThat(tableInfo.getTableConstraints().getPrimaryKey().getColumns())
                .containsExactly("event_ts", "tenant");
    }

    @Test
    void buildsMaximumStalenessDdlAtMicrosecondPrecision() {
        QueryJobConfiguration query =
                BigQueryTableAdmin.alterMaxStalenessQuery(
                        TableDestination.of("my-project", "analytics", "orders"),
                        Duration.ofNanos(600_000_001_000L));

        assertThat(query.getQuery())
                .isEqualTo(
                        "ALTER TABLE `my-project.analytics.orders` SET OPTIONS"
                                + " (max_staleness = INTERVAL 600000001 MICROSECOND)");
        assertThat(query.useLegacySql()).isFalse();
    }

    @Test
    void clearsMaximumStalenessWithNull() {
        QueryJobConfiguration query = BigQueryTableAdmin.alterMaxStalenessQuery(DESTINATION, null);

        assertThat(query.getQuery())
                .isEqualTo("ALTER TABLE `p.d.t` SET OPTIONS (max_staleness = NULL)");
    }

    @Test
    void informationSchemaCheckBindsTheTableName() {
        QueryJobConfiguration query =
                BigQueryTableAdmin.maxStalenessCheckQuery(
                        TableDestination.of("my-project", "analytics", "orders' OR TRUE"),
                        Duration.ofMinutes(10));

        assertThat(query.getQuery())
                .contains("`my-project.analytics.INFORMATION_SCHEMA.TABLE_OPTIONS`")
                .contains("table_name = @table_name")
                .contains("INTERVAL 600000000 MICROSECOND")
                .doesNotContain("orders' OR TRUE");
        assertThat(query.getNamedParameters().get("table_name").getValue())
                .isEqualTo("orders' OR TRUE");
    }

    @Test
    void informationSchemaTreatsAbsenceOrTheZeroIntervalAsCleared() {
        QueryJobConfiguration query = BigQueryTableAdmin.maxStalenessCheckQuery(DESTINATION, null);

        assertThat(query.getQuery())
                .contains(
                        "(COUNT(*) = 0 OR (COUNT(*) = 1 AND COUNTIF(CAST(option_value AS"
                                + " INTERVAL) = INTERVAL 0 MICROSECOND) = 1)) AS matches")
                .contains("option_name = 'max_staleness'");
    }

    @Test
    void mergeSchemaPreservesRestOnlyAttributesOfExistingFields() {
        com.google.cloud.bigquery.Schema existing =
                com.google.cloud.bigquery.Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                                        "name",
                                        com.google.cloud.bigquery.StandardSQLTypeName.STRING)
                                .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                                .setDescription("the name")
                                .setPolicyTags(
                                        com.google.cloud.bigquery.PolicyTags.newBuilder()
                                                .setNames(
                                                        java.util.List.of(
                                                                "projects/p/locations/l/taxonomies"
                                                                        + "/t/policyTags/pii"))
                                                .build())
                                .build());
        TableSchema proposed =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("name")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("email")
                                        .setType(TableFieldSchema.Type.STRING)
                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                        .build();

        com.google.cloud.bigquery.Schema merged =
                BigQueryTableAdmin.mergeSchema(existing, proposed);

        com.google.cloud.bigquery.Field name = merged.getFields().get(0);
        // Relaxation applied, everything the Storage form cannot express preserved.
        assertThat(name.getMode()).isEqualTo(com.google.cloud.bigquery.Field.Mode.NULLABLE);
        assertThat(name.getDescription()).isEqualTo("the name");
        assertThat(name.getPolicyTags().getNames()).isNotEmpty();
        assertThat(merged.getFields().get(1).getName()).isEqualTo("email");
        assertThat(merged.getFields()).hasSize(2);
    }

    @Test
    void mergeSchemaRecursesIntoStructs() {
        com.google.cloud.bigquery.Schema existing =
                com.google.cloud.bigquery.Schema.of(
                        com.google.cloud.bigquery.Field.newBuilder(
                                        "address",
                                        com.google.cloud.bigquery.StandardSQLTypeName.STRUCT,
                                        com.google.cloud.bigquery.FieldList.of(
                                                com.google.cloud.bigquery.Field.newBuilder(
                                                                "city",
                                                                com.google.cloud.bigquery
                                                                        .StandardSQLTypeName.STRING)
                                                        .setDescription("the city")
                                                        .build()))
                                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE)
                                .build());
        TableSchema proposed =
                TableSchema.newBuilder()
                        .addFields(
                                TableFieldSchema.newBuilder()
                                        .setName("address")
                                        .setType(TableFieldSchema.Type.STRUCT)
                                        .setMode(TableFieldSchema.Mode.NULLABLE)
                                        .addFields(
                                                TableFieldSchema.newBuilder()
                                                        .setName("city")
                                                        .setType(TableFieldSchema.Type.STRING)
                                                        .setMode(TableFieldSchema.Mode.NULLABLE))
                                        .addFields(
                                                TableFieldSchema.newBuilder()
                                                        .setName("zip")
                                                        .setType(TableFieldSchema.Type.STRING)
                                                        .setMode(TableFieldSchema.Mode.NULLABLE)))
                        .build();

        com.google.cloud.bigquery.Schema merged =
                BigQueryTableAdmin.mergeSchema(existing, proposed);

        com.google.cloud.bigquery.FieldList subFields = merged.getFields().get(0).getSubFields();
        assertThat(subFields).hasSize(2);
        assertThat(subFields.get(0).getDescription()).isEqualTo("the city");
        assertThat(subFields.get(1).getName()).isEqualTo("zip");
    }

    @Test
    void lostRacesAreRecognizedByHttpCode() {
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(409, "conflict"))).isTrue();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(412, "precondition failed")))
                .isTrue();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(403, "forbidden")))
                .isFalse();
        assertThat(BigQueryTableAdmin.isLostRace(new BigQueryException(400, "bad request")))
                .isFalse();
    }

    @Test
    void lostRacesAreRecognizedByErrorReason() {
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        400,
                                        "etag mismatch",
                                        new BigQueryError("conditionNotMet", null, "etag"))))
                .isTrue();
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        403,
                                        "quota",
                                        new BigQueryError("rateLimitExceeded", null, "quota"))))
                .isTrue();
        assertThat(
                        BigQueryTableAdmin.isLostRace(
                                new BigQueryException(
                                        403,
                                        "denied",
                                        new BigQueryError("accessDenied", null, "denied"))))
                .isFalse();
    }

    @Test
    void theRateLimitedCreationRaceIsRetriable() {
        // Measured 2026-08-08: sixteen concurrent creations of one missing table, five of them
        // answered exactly this — HTTP 403, reason rateLimitExceeded, "Exceeded rate limits: too
        // many table update operations for this table". Not a 409, so nothing else lets it through.
        assertThat(
                        BigQueryTableAdmin.isRetriable(
                                new BigQueryException(
                                        403,
                                        "Exceeded rate limits: too many table update operations"
                                                + " for this table.",
                                        new BigQueryError(
                                                "rateLimitExceeded",
                                                null,
                                                "Exceeded rate limits"))))
                .isTrue();
    }

    @Test
    void aTransientCdcTableReadIsRetriable() {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(
                StubBigQuery.TableAnswer.failing(
                        new BigQueryException(503, "temporarily unavailable")));

        assertThatThrownBy(() -> new BigQueryTableAdmin(client).read(DESTINATION))
                .isExactlyInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("Failed to read BigQuery table")
                .hasCauseInstanceOf(BigQueryException.class);
    }

    /** A live table's definition, for the paths that only read metadata beside it. */
    private static final StandardTableDefinition NO_COLUMNS =
            StandardTableDefinition.of(com.google.cloud.bigquery.Schema.of());

    @Test
    void theCdcStateCarriesThePrimaryKeyProvisioningLabelAndEtag() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(
                StubBigQuery.TableAnswer.existing(
                        NO_COLUMNS,
                        "etag-7",
                        Map.of("flink_gcp_cdc", "complete_spec", "owner", "ops"),
                        Arrays.asList("id", "tenant")));

        CdcTableProvisioner.TableState state = new BigQueryTableAdmin(client).read(DESTINATION);

        assertThat(state).isNotNull();
        assertThat(state.primaryKeyColumns()).containsExactly("id", "tenant");
        assertThat(state.provisioningLabel()).isEqualTo("complete_spec");
        assertThat(state.etag()).isEqualTo("etag-7");
    }

    @Test
    void aTableWithNeitherConstraintsNorLabelsReadsAsUnprovisioned() throws Exception {
        // The shape of a table someone else created: the provisioner must see "no primary key,
        // no label" rather than a partially-read state it would mistake for one of its own.
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(StubBigQuery.TableAnswer.existing(NO_COLUMNS, "etag-1", null, null));

        CdcTableProvisioner.TableState state = new BigQueryTableAdmin(client).read(DESTINATION);

        assertThat(state).isNotNull();
        assertThat(state.primaryKeyColumns()).isEmpty();
        assertThat(state.provisioningLabel()).isNull();
    }

    @Test
    void anAbsentCdcTableReadsAsNoState() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(StubBigQuery.TableAnswer.absent());

        assertThat(new BigQueryTableAdmin(client).read(DESTINATION)).isNull();
    }

    @Test
    void anAbsentTableHasNoSchemaSnapshotRatherThanAFailure() throws Exception {
        // The writers' auto-creation path asks for the schema first and treats null as "create
        // it"; a failure here instead would fail the job on the table's very first checkpoint.
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(StubBigQuery.TableAnswer.absent());

        assertThat(new BigQueryTableAdmin(client).getSchema(DESTINATION)).isNull();
    }

    @Test
    void aSchemaTheConverterRejectsSurfacesAsTheSpiFailure() {
        // The SPI promises IOException; a converter meeting a column it cannot describe throws an
        // unchecked one, which would otherwise travel straight past every caller's catch — the
        // same shape as the load runner's `Job#reload()` defect (#337). The driver is a `RANGE`
        // column over an element type the Storage enum has no name for, which is what a REST
        // response naming a type this build predates would look like. It is also the only such
        // response the vendor's model can carry: the `default:` arm of the type switch guards a
        // `Field` that cannot be built at all, since `Field.newBuilder(..., ARRAY).build()` fails
        // inside the vendor's own constructor.
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(
                StubBigQuery.TableAnswer.existing(
                        StandardTableDefinition.of(
                                com.google.cloud.bigquery.Schema.of(
                                        com.google.cloud.bigquery.Field.newBuilder(
                                                        "unreadable-column",
                                                        com.google.cloud.bigquery
                                                                .StandardSQLTypeName.RANGE)
                                                .setRangeElementType(
                                                        com.google.cloud.bigquery.FieldElementType
                                                                .newBuilder()
                                                                .setType("TYPE_FROM_THE_FUTURE")
                                                                .build())
                                                .build())),
                        "etag-1",
                        null,
                        null));

        assertThatThrownBy(() -> new BigQueryTableAdmin(client).getSchema(DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("to its Storage API form")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void aLostSchemaUpdateRaceAsksForAFreshReadRatherThanFailingTheJob() throws Exception {
        // `RetryingTableAdmin` passes `updateSchema` through unretried (ADR-0071), because `false`
        // means "re-read and derive again" — repeating a proposal built against a snapshot now
        // known to be stale is what must not happen.
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(
                StubBigQuery.TableAnswer.existing(NO_COLUMNS, "stale-etag", null, null));
        client.updateTableFailure = new BigQueryException(412, "precondition failed");
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);
        TableSchemaSnapshot snapshot = admin.getSchema(DESTINATION);

        assertThat(admin.updateSchema(DESTINATION, snapshot, SCHEMA)).isFalse();
        // The submitted table carries the snapshot's etag, which is what makes the update
        // conditional at all: without it the service would accept it over a concurrent change.
        assertThat(client.updatedTables)
                .singleElement()
                .satisfies(
                        submitted -> {
                            assertThat(submitted.getEtag()).isEqualTo("stale-etag");
                            assertThat(
                                            submitted
                                                    .<StandardTableDefinition>getDefinition()
                                                    .getSchema()
                                                    .getFields())
                                    .extracting(com.google.cloud.bigquery.Field::getName)
                                    .containsExactly("event_ts");
                        });
    }

    @Test
    void aRejectedSchemaUpdateStaysTerminal() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.tablesAnswering(StubBigQuery.TableAnswer.existing(NO_COLUMNS, "etag-1", null, null));
        client.updateTableFailure = new BigQueryException(400, "invalid schema change");
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);
        TableSchemaSnapshot snapshot = admin.getSchema(DESTINATION);

        assertThatThrownBy(() -> admin.updateSchema(DESTINATION, snapshot, SCHEMA))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("Failed to update the schema");
    }

    @Test
    void updateProvisioningLabelCarriesTheVerifiedEtagAndOnlyLabelsThroughTheProductionPath()
            throws Exception {
        MockLowLevelHttpRequest request =
                new MockLowLevelHttpRequest()
                        .setResponse(new MockLowLevelHttpResponse().setStatusCode(200));
        BigQueryTableAdmin admin = completionAdmin(request);

        assertThat(
                        admin.updateProvisioningLabel(
                                DESTINATION, "pending_spec", "complete_spec", "verified-etag"))
                .isTrue();

        assertThat(request.getUrl())
                .isEqualTo("https://bigquery.example/bigquery/v2/projects/p/datasets/d/tables/t");
        assertThat(request.getFirstHeaderValue("X-HTTP-Method-Override")).isEqualTo("PATCH");
        assertThat(request.getFirstHeaderValue("If-Match")).isEqualTo("verified-etag");
        assertThat(request.getContentAsString())
                .startsWith("{\"labels\":{")
                .contains("\"flink_gcp_cdc\":\"complete_spec\"")
                .contains("\"owner\":\"ops\"")
                .doesNotContain("etag");
    }

    @Test
    void aCompletionLabelPreconditionFailureReportsALostRace() throws Exception {
        MockLowLevelHttpRequest request =
                new MockLowLevelHttpRequest()
                        .setResponse(new MockLowLevelHttpResponse().setStatusCode(412));

        assertThat(
                        completionAdmin(request)
                                .updateProvisioningLabel(
                                        DESTINATION,
                                        "pending_spec",
                                        "complete_spec",
                                        "verified-etag"))
                .isFalse();
    }

    @Test
    void aLabelThatMovedOnSinceItWasReadIsNeverPatched() throws Exception {
        // The claim is checked before the request is built, so a label another writer already
        // changed loses locally: the patch that would overwrite it is never sent at all. The
        // request is left with the mock's default 200, so dropping the check would answer `true`
        // here rather than failing for a reason of its own.
        MockLowLevelHttpRequest request = new MockLowLevelHttpRequest();

        assertThat(
                        completionAdmin(request)
                                .updateProvisioningLabel(
                                        DESTINATION,
                                        "pending_someone_elses_spec",
                                        "complete_spec",
                                        "verified-etag"))
                .isFalse();
        assertThat(request.getUrl()).isNull();
    }

    @Test
    void aMissingTableDuringTheCompletionPatchReportsALostRace() throws Exception {
        MockLowLevelHttpRequest request =
                new MockLowLevelHttpRequest()
                        .setResponse(new MockLowLevelHttpResponse().setStatusCode(404));

        assertThat(
                        completionAdmin(request)
                                .updateProvisioningLabel(
                                        DESTINATION,
                                        "pending_spec",
                                        "complete_spec",
                                        "verified-etag"))
                .isFalse();
    }

    @Test
    void transientCompletionLabelFailuresAreRetriable() {
        assertCompletionFailureIsRetriable(
                new MockLowLevelHttpResponse()
                        .setStatusCode(403)
                        .setContentType("application/json")
                        .setContent(
                                "{\"error\":{\"errors\":[{\"reason\":\"rateLimitExceeded\"}]}}"));
        assertCompletionFailureIsRetriable(new MockLowLevelHttpResponse().setStatusCode(503));
    }

    @Test
    void anAmbiguousCompletionLabelTransportFailureIsRetriable() {
        MockLowLevelHttpRequest request =
                new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        throw new IOException("connection reset after request");
                    }
                };

        assertThatThrownBy(
                        () ->
                                completionAdmin(request)
                                        .updateProvisioningLabel(
                                                DESTINATION,
                                                "pending_spec",
                                                "complete_spec",
                                                "verified-etag"))
                .isExactlyInstanceOf(RetriableTableAdminException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void aTerminalCompletionLabelFailureStaysTerminal() {
        MockLowLevelHttpRequest request =
                new MockLowLevelHttpRequest()
                        .setResponse(
                                new MockLowLevelHttpResponse()
                                        .setStatusCode(403)
                                        .setContentType("application/json")
                                        .setContent(
                                                "{\"error\":{\"errors\":[{\"reason\":\"accessDenied\"}]}}"));

        assertThatThrownBy(
                        () ->
                                completionAdmin(request)
                                        .updateProvisioningLabel(
                                                DESTINATION,
                                                "pending_spec",
                                                "complete_spec",
                                                "verified-etag"))
                .isExactlyInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class);
    }

    private static void assertCompletionFailureIsRetriable(MockLowLevelHttpResponse response) {
        MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
        assertThatThrownBy(
                        () ->
                                completionAdmin(request)
                                        .updateProvisioningLabel(
                                                DESTINATION,
                                                "pending_spec",
                                                "complete_spec",
                                                "verified-etag"))
                .isExactlyInstanceOf(RetriableTableAdminException.class);
    }

    private static BigQueryTableAdmin completionAdmin(MockLowLevelHttpRequest request) {
        MockHttpTransport transport =
                new MockHttpTransport() {
                    @Override
                    public MockLowLevelHttpRequest buildRequest(String method, String url) {
                        assertThat(method).isEqualTo("POST");
                        return request.setUrl(url);
                    }
                };
        BigQueryOptions options =
                BigQueryOptions.newBuilder()
                        .setProjectId("p")
                        .setHost("https://bigquery.example")
                        .setCredentials(NoCredentials.getInstance())
                        .setTransportOptions(
                                HttpTransportOptions.newBuilder()
                                        .setHttpTransportFactory(() -> transport)
                                        .build())
                        .build();
        StubBigQuery client = new StubBigQuery(options);
        client.tablesAnswering(
                StubBigQuery.TableAnswer.existing(
                        "current-etag", Map.of("flink_gcp_cdc", "pending_spec", "owner", "ops")));
        return new BigQueryTableAdmin(client);
    }

    @Test
    void documentedTransientQueryJobReasonsAreRetriable() {
        assertThat(
                        Set.of(
                                        "rateLimitExceeded",
                                        "jobRateLimitExceeded",
                                        "backendError",
                                        "internalError",
                                        "jobBackendError",
                                        "jobInternalError")
                                .stream()
                                .allMatch(BigQueryTableAdmin::isRetriableJobReason))
                .isTrue();
        assertThat(BigQueryTableAdmin.isRetriableJobReason("invalidQuery")).isFalse();
        assertThat(BigQueryTableAdmin.isRetriableJobReason("accessDenied")).isFalse();
    }

    @Test
    void directQueryFailuresUseTheSameTransientReasonsAsPolledJobs() {
        assertThat(
                        BigQueryTableAdmin.isRetriableQueryFailure(
                                new BigQueryException(
                                        Arrays.asList(
                                                new BigQueryError(
                                                        "jobBackendError",
                                                        null,
                                                        "transient query failure")))))
                .isTrue();
        assertThat(
                        BigQueryTableAdmin.isRetriableQueryFailure(
                                new BigQueryException(
                                        Arrays.asList(
                                                new BigQueryError(
                                                        "accessDenied", null, "terminal")))))
                .isFalse();
        assertThat(
                        BigQueryTableAdmin.isRetriableQueryFailure(
                                new BigQueryException(400, "terminal without structured errors")))
                .isFalse();
    }

    @Test
    void theDirectQueryPathSurfacesTransientReasonsToTheProvisioningRetry() {
        StubBigQuery client = new StubBigQuery();
        client.queryFailure =
                new BigQueryException(
                        Arrays.asList(
                                new BigQueryError(
                                        "jobRateLimitExceeded", null, "retry this query")));

        assertThatThrownBy(
                        () ->
                                new BigQueryTableAdmin(client)
                                        .maxStalenessMatches(DESTINATION, Duration.ofMinutes(10)))
                .isExactlyInstanceOf(RetriableTableAdminException.class)
                .hasCause(client.queryFailure);
    }

    @Test
    void theSdkDoesNotConsiderTheRateLimitedCreationRaceRetryable() {
        // Why the connector needs a rule of its own at all: BigQueryImpl.create runs under
        // runWithRetries, but the handler consults isRetryable(), whose RETRYABLE_ERRORS set is
        // 500/502/503/504 alone. Measured false on the real failure; pinned so a client release
        // that starts retrying it is noticed here rather than by two layers retrying at once.
        assertThat(
                        new BigQueryException(
                                        403,
                                        "Exceeded rate limits",
                                        new BigQueryError(
                                                "rateLimitExceeded", null, "Exceeded rate limits"))
                                .isRetryable())
                .isFalse();
    }

    @Test
    void retriableCreationFailuresAreRecognizedByHttpCode() {
        assertThat(BigQueryTableAdmin.isRetriable(new BigQueryException(429, "too many requests")))
                .isTrue();
        // Borrowed from the client rather than restated: 503 is in its own RETRYABLE_ERRORS.
        assertThat(BigQueryTableAdmin.isRetriable(new BigQueryException(503, "unavailable")))
                .isTrue();
        assertThat(BigQueryTableAdmin.isRetriable(new BigQueryException(403, "forbidden")))
                .isFalse();
        assertThat(BigQueryTableAdmin.isRetriable(new BigQueryException(400, "bad request")))
                .isFalse();
    }

    @Test
    void theOtherQuotaReasonIsNotRetriable() {
        // quotaExceeded is left out on the widen-only-what-was-observed rule: the measurement
        // answered rateLimitExceeded, and BigQuery attaches this one to quotas refilling on
        // boundaries no connector budget outwaits as well as to rates.
        assertThat(
                        BigQueryTableAdmin.isRetriable(
                                new BigQueryException(
                                        403,
                                        "quota",
                                        new BigQueryError("quotaExceeded", null, "quota"))))
                .isFalse();
    }

    @Test
    void aFailedCreationIsTypedByWhetherRepeatingItCanHelp() {
        BigQueryException rateLimited =
                new BigQueryException(
                        403, "quota", new BigQueryError("rateLimitExceeded", null, "quota"));
        BigQueryException denied =
                new BigQueryException(
                        403, "denied", new BigQueryError("accessDenied", null, "denied"));

        assertThat(BigQueryTableAdmin.toFailure(DESTINATION, rateLimited))
                .isInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("p.d.t")
                .hasCause(rateLimited);
        assertThat(BigQueryTableAdmin.toFailure(DESTINATION, denied))
                .isExactlyInstanceOf(IOException.class)
                .hasMessageContaining("p.d.t")
                .hasCause(denied);
    }

    @Test
    void createTypesTheClientsFailureRatherThanFlatteningIt() throws Exception {
        // The link the two cases above do not reach: create's catch has to hand its failure to
        // toFailure. Wrapping it in a plain IOException instead compiles, keeps every other test
        // green, and silently restores the defect #383 fixes — the writers would stop retrying a
        // lost creation race, because they route on the type alone.
        StubBigQuery client = new StubBigQuery();
        client.createTableFailure =
                new BigQueryException(
                        403, "quota", new BigQueryError("rateLimitExceeded", null, "quota"));
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);

        assertThatThrownBy(() -> admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults()))
                .isInstanceOf(RetriableTableAdminException.class);
        assertThat(client.createdTables).hasSize(1);
    }

    @Test
    void createLeavesATerminalClientFailureTerminal() {
        StubBigQuery client = new StubBigQuery();
        client.createTableFailure =
                new BigQueryException(
                        403, "denied", new BigQueryError("accessDenied", null, "denied"));
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);

        assertThatThrownBy(() -> admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults()))
                .isExactlyInstanceOf(IOException.class);
    }

    @Test
    void createTreatsAConflictAsSuccess() throws Exception {
        // The oldest rule in this class, and until now it had no unit test either: a subtask that
        // lost the creation race to another subtask's completed creation has nothing left to do.
        StubBigQuery client = new StubBigQuery();
        client.createTableFailure = new BigQueryException(409, "Already Exists");
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);

        admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults());

        assertThat(client.createdTables).hasSize(1);
    }

    @Test
    void cdcTryCreateReportsALostRaceRatherThanSucceeding() throws Exception {
        StubBigQuery client = new StubBigQuery();
        client.createTableFailure = new BigQueryException(409, "Already Exists");
        BigQueryTableAdmin admin = new BigQueryTableAdmin(client);

        assertThat(
                        admin.tryCreate(
                                DESTINATION,
                                SCHEMA,
                                TableCreateOptions.defaults(),
                                CdcTableOptions.builder()
                                        .primaryKeyColumns(Arrays.asList("id"))
                                        .build(),
                                "pending_specification"))
                .isFalse();

        assertThat(client.createdTables).hasSize(1);
    }

    @Test
    void emulatorOptionsCarryTheHostTheProjectAndNoCredentials() {
        BigQueryOptions options =
                BigQueryTableAdmin.emulatorOptions(
                        EmulatorEndpoint.parse("localhost:9050"), DESTINATION.getProject());

        assertThat(options.getHost()).isEqualTo("http://localhost:9050");
        // The project id is load-bearing rather than cosmetic: BigQueryOptions refuses to build
        // without one it can determine, and an emulator offers no environment to determine one
        // from — so leaving it unset fails on a machine with no gcloud configuration and passes on
        // one with it. Asserting it here is what makes that guard independent of the machine the
        // test runs on; the integration tests only catch it on a runner.
        assertThat(options.getProjectId()).isEqualTo("p");
        assertThat(options.getCredentials()).isInstanceOf(NoCredentials.class);
    }
}
