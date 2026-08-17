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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.escape.CharEscapers;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.http.HttpTransportOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link TableAdmin} backed by the BigQuery REST client.
 *
 * <p>The client is created lazily on the first use, so jobs whose destination tables all exist and
 * never evolve their schemas never construct it. HTTP conflicts on creation (409, the table was
 * created concurrently — for example by a parallel subtask) are treated as success; a creation that
 * lost the same race to the per-table metadata-update quota instead ({@link #isRetriable}) is typed
 * as a {@link RetriableTableAdminException} for the caller to repeat.
 *
 * <p>Schema updates are etag-conditioned: {@link #getSchema} snapshots the REST {@code Table}
 * (which carries the etag), and {@link #updateSchema} submits the modified table so BigQuery
 * rejects the update when the table changed since the snapshot. Lost races — the etag precondition
 * failing, a concurrent-modification conflict, or the per-table metadata-update quota (about five
 * updates per ten seconds) being momentarily exceeded — are reported as {@code false} for the
 * caller to re-read and retry. The updated schema is assembled by <em>merging</em> the proposed
 * Storage-form schema onto the snapshot's REST fields, so REST-only column attributes the Storage
 * form cannot represent (policy tags, collation, ...) are preserved for existing columns.
 *
 * <p>CDC creation uses the same REST client for schema, primary key, and provisioning labels.
 * Because BigQuery does not store {@code maxStaleness} from Tables API writes, a configured value
 * is applied through GoogleSQL and verified through {@code INFORMATION_SCHEMA.TABLE_OPTIONS} before
 * the provisioning label becomes complete.
 *
 * <p>(The lost-race handling follows the coordinator-free concurrent-update pattern of the
 * Aiven/kafka-connect-bigquery connector, reimplemented independently; see the module README's
 * provenance section.)
 */
@Internal
public class BigQueryTableAdmin implements TableAdmin, CdcTableProvisioner.Service {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableAdmin.class);

    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_PRECONDITION_FAILED = 412;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    /** Google API method override header used because the default JDK transport rejects PATCH. */
    private static final String METHOD_OVERRIDE_HEADER = "X-HTTP-Method-Override";

    /** Error reason of a failed etag precondition. */
    private static final String REASON_CONDITION_NOT_MET = "conditionNotMet";

    /** Error reason of the per-table metadata-update quota. */
    private static final String REASON_RATE_LIMIT_EXCEEDED = "rateLimitExceeded";

    private static final Pattern RATE_LIMIT_REASON =
            Pattern.compile("\\\"reason\\\"\\s*:\\s*\\\"rateLimitExceeded\\\"");

    private static final Set<String> RETRIABLE_JOB_REASONS =
            Set.of(
                    REASON_RATE_LIMIT_EXCEEDED,
                    "jobRateLimitExceeded",
                    "backendError",
                    "internalError",
                    "jobBackendError",
                    "jobInternalError");

    private BigQuery client;

    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final String location;
    private final CdcTableProvisioner cdcTableProvisioner;

    /** Creates an admin using application-default credentials. */
    public BigQueryTableAdmin() {
        this(null, null, null);
    }

    /**
     * Creates an admin talking to a BigQuery emulator's REST endpoint with no credentials, or —
     * when the endpoint is {@code null} — to the production service with application-default
     * credentials.
     *
     * @param emulatorEndpoint the emulator's REST endpoint as {@code host:port}, or {@code null}
     */
    public BigQueryTableAdmin(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(null, emulatorEndpoint, null);
    }

    /** Creates an admin with optional runtime-loaded production credentials. */
    public BigQueryTableAdmin(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(serviceAccountKeyFile, emulatorEndpoint, null);
    }

    /** Creates an admin with optional credentials, emulator endpoint, and query-job location. */
    public BigQueryTableAdmin(
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable String location) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.location = location;
        this.cdcTableProvisioner = new CdcTableProvisioner(this);
    }

    /** Returns the configured key-file path, or {@code null} for ADC. */
    @VisibleForTesting
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /**
     * Creates an admin using the given client.
     *
     * @param client the BigQuery REST client
     */
    public BigQueryTableAdmin(BigQuery client) {
        this.client = client;
        this.serviceAccountKeyFile = null;
        this.emulatorEndpoint = null;
        this.location = null;
        this.cdcTableProvisioner = new CdcTableProvisioner(this);
    }

    @Override
    public void create(TableDestination destination, TableSchema schema, TableCreateOptions options)
            throws IOException {
        TableInfo tableInfo = buildTableInfo(destination, schema, options);
        try {
            client(destination).create(tableInfo);
            LOG.info("Created BigQuery table {} with options {}", destination, options);
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_CONFLICT) {
                LOG.info("BigQuery table {} already exists, not creating it", destination);
                return;
            }
            throw toFailure(destination, e);
        }
    }

    @Override
    public boolean ensureCdcTable(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider createOptionsProvider,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy reconciliationPolicy)
            throws IOException {
        return cdcTableProvisioner.ensure(
                destination,
                schema,
                createOptionsProvider,
                cdcOptions,
                createDisposition,
                reconciliationPolicy);
    }

    @Override
    @Nullable
    public CdcTableProvisioner.TableState read(TableDestination destination) throws IOException {
        Table table;
        try {
            table = client(destination).getTable(toTableId(destination));
        } catch (BigQueryException e) {
            throw toCdcFailure("read", destination, e);
        }
        if (table == null) {
            return null;
        }
        List<String> primaryKeyColumns = Collections.emptyList();
        TableConstraints constraints = table.getTableConstraints();
        if (constraints != null && constraints.getPrimaryKey() != null) {
            primaryKeyColumns = constraints.getPrimaryKey().getColumns();
        }
        Map<String, String> labels = table.getLabels();
        return new CdcTableProvisioner.TableState(
                primaryKeyColumns,
                labels == null ? null : labels.get(CdcTableProvisioner.PROVISIONING_LABEL),
                table.getEtag());
    }

    @Override
    public boolean tryCreate(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptions createOptions,
            CdcTableOptions cdcOptions,
            String provisioningLabel)
            throws IOException {
        Map<String, String> labels =
                Collections.singletonMap(CdcTableProvisioner.PROVISIONING_LABEL, provisioningLabel);
        TableInfo tableInfo =
                buildTableInfo(destination, schema, createOptions, cdcOptions).toBuilder()
                        .setLabels(labels)
                        .build();
        try {
            client(destination).create(tableInfo);
            LOG.info(
                    "Created BigQuery CDC table {} with options {} and provisioning label {}",
                    destination,
                    cdcOptions,
                    provisioningLabel);
            return true;
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_CONFLICT) {
                LOG.info("BigQuery CDC table {} already exists", destination);
                return false;
            }
            throw toFailure(destination, e);
        }
    }

    @Override
    public void setMaxStaleness(TableDestination destination, @Nullable Duration maxStaleness)
            throws IOException {
        runQuery(
                destination,
                alterMaxStalenessQuery(destination, maxStaleness),
                "set max_staleness");
        LOG.info("Set max_staleness={} on BigQuery CDC table {}", maxStaleness, destination);
    }

    @Override
    public boolean maxStalenessMatches(
            TableDestination destination, @Nullable Duration maxStaleness) throws IOException {
        TableResult result =
                runQuery(
                        destination,
                        maxStalenessCheckQuery(destination, maxStaleness),
                        "verify max_staleness");
        java.util.Iterator<FieldValueList> rows = result.iterateAll().iterator();
        if (!rows.hasNext()) {
            throw new IOException(
                    "INFORMATION_SCHEMA returned no verification row for BigQuery table "
                            + destination);
        }
        boolean matches = rows.next().get("matches").getBooleanValue();
        if (rows.hasNext()) {
            throw new IOException(
                    "INFORMATION_SCHEMA returned more than one verification row for BigQuery"
                            + " table "
                            + destination);
        }
        return matches;
    }

    @Override
    public boolean updateProvisioningLabel(
            TableDestination destination,
            @Nullable String expectedLabel,
            String nextLabel,
            String verifiedEtag)
            throws IOException {
        Table table;
        try {
            table = client(destination).getTable(toTableId(destination));
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_NOT_FOUND) {
                return false;
            }
            throw toCdcFailure("read provisioning state of", destination, e);
        }
        if (table == null) {
            return false;
        }
        Map<String, String> labels =
                table.getLabels() == null ? new HashMap<>() : new HashMap<>(table.getLabels());
        if (!Objects.equals(expectedLabel, labels.get(CdcTableProvisioner.PROVISIONING_LABEL))) {
            return false;
        }
        labels.put(CdcTableProvisioner.PROVISIONING_LABEL, nextLabel);
        try {
            conditionalLabelPatch(
                    client(destination).getOptions(), destination, verifiedEtag, labels);
            LOG.info(
                    "Changed BigQuery CDC table {} provisioning label from {} to {}",
                    destination,
                    expectedLabel,
                    nextLabel);
            return true;
        } catch (HttpResponseException e) {
            if (e.getStatusCode() == HTTP_CONFLICT
                    || e.getStatusCode() == HTTP_PRECONDITION_FAILED
                    || e.getStatusCode() == HTTP_NOT_FOUND) {
                return false;
            }
            String message =
                    "Failed to update CDC provisioning state for BigQuery table " + destination;
            if (isRetriable(e)) {
                throw new RetriableTableAdminException(message, e);
            }
            throw new IOException(message, e);
        } catch (IOException e) {
            throw new RetriableTableAdminException(
                    "Failed to update CDC provisioning state for BigQuery table " + destination, e);
        }
    }

    @VisibleForTesting
    static void conditionalLabelPatch(
            BigQueryOptions options,
            TableDestination destination,
            String etag,
            Map<String, String> labels)
            throws IOException {
        Preconditions.checkNotNull(etag, "BigQuery table etag must not be null");
        HttpTransportOptions transportOptions =
                (HttpTransportOptions) options.getTransportOptions();
        String url =
                stripTrailingSlash(options.getHost())
                        + "/bigquery/v2/projects/"
                        + path(destination.getProject())
                        + "/datasets/"
                        + path(destination.getDataset())
                        + "/tables/"
                        + path(destination.getTable());
        HttpRequest request =
                transportOptions
                        .getHttpTransportFactory()
                        .create()
                        .createRequestFactory(transportOptions.getHttpRequestInitializer(options))
                        .buildPostRequest(
                                new GenericUrl(url),
                                new JsonHttpContent(
                                        GsonFactory.getDefaultInstance(),
                                        Collections.singletonMap("labels", labels)));
        request.getHeaders().set(METHOD_OVERRIDE_HEADER, "PATCH");
        request.getHeaders().setIfMatch(etag);
        HttpResponse response = request.execute();
        try {
            // Executing the conditional patch is the operation; no response fields are needed.
        } finally {
            response.disconnect();
        }
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static String path(String value) {
        return CharEscapers.escapeUriPath(value);
    }

    private static boolean isRetriable(HttpResponseException e) {
        int status = e.getStatusCode();
        return status == HTTP_TOO_MANY_REQUESTS
                || status >= 500
                || (status == 403
                        && e.getContent() != null
                        && RATE_LIMIT_REASON.matcher(e.getContent()).find());
    }

    @VisibleForTesting
    static QueryJobConfiguration alterMaxStalenessQuery(
            TableDestination destination, @Nullable Duration maxStaleness) {
        String value =
                maxStaleness == null
                        ? "NULL"
                        : "INTERVAL " + maxStaleness.toNanos() / 1_000 + " MICROSECOND";
        return QueryJobConfiguration.newBuilder(
                        "ALTER TABLE "
                                + quotedTable(destination)
                                + " SET OPTIONS (max_staleness = "
                                + value
                                + ")")
                .setUseLegacySql(false)
                .build();
    }

    @VisibleForTesting
    static QueryJobConfiguration maxStalenessCheckQuery(
            TableDestination destination, @Nullable Duration maxStaleness) {
        String predicate =
                maxStaleness == null
                        ? "(COUNT(*) = 0 OR (COUNT(*) = 1 AND COUNTIF(CAST(option_value AS"
                                + " INTERVAL) = INTERVAL 0 MICROSECOND) = 1))"
                        : "COUNT(*) = 1 AND COUNTIF(CAST(option_value AS INTERVAL) = INTERVAL "
                                + maxStaleness.toNanos() / 1_000
                                + " MICROSECOND) = 1";
        return QueryJobConfiguration.newBuilder(
                        "SELECT "
                                + predicate
                                + " AS matches FROM "
                                + quotedOptionsView(destination)
                                + " WHERE table_name = @table_name"
                                + " AND option_name = 'max_staleness'")
                .setUseLegacySql(false)
                .addNamedParameter("table_name", QueryParameterValue.string(destination.getTable()))
                .build();
    }

    private TableResult runQuery(
            TableDestination destination, QueryJobConfiguration query, String operation)
            throws IOException {
        JobId.Builder job = JobId.newBuilder().setProject(destination.getProject());
        if (location != null) {
            job.setLocation(location);
        }
        try {
            return client(destination).query(query, job.build());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while trying to "
                            + operation
                            + " for BigQuery table "
                            + destination,
                    e);
        } catch (JobException e) {
            if (isRetriable(e)) {
                throw new RetriableTableAdminException(
                        "Failed to " + operation + " for BigQuery table " + destination, e);
            }
            throw new IOException(
                    "Failed to " + operation + " for BigQuery table " + destination, e);
        } catch (BigQueryException e) {
            String message = "Failed to " + operation + " for BigQuery table " + destination;
            if (isRetriableQueryFailure(e)) {
                throw new RetriableTableAdminException(message, e);
            }
            throw new IOException(message, e);
        }
    }

    @VisibleForTesting
    static boolean isRetriable(JobException e) {
        return e.getErrors().stream().anyMatch(error -> isRetriableJobReason(error.getReason()));
    }

    @VisibleForTesting
    static boolean isRetriableQueryFailure(BigQueryException e) {
        return isRetriable(e)
                || (e.getErrors() != null
                        && e.getErrors().stream()
                                .anyMatch(error -> isRetriableJobReason(error.getReason())));
    }

    @VisibleForTesting
    static boolean isRetriableJobReason(String reason) {
        return RETRIABLE_JOB_REASONS.contains(reason);
    }

    private static IOException toCdcFailure(
            String operation, TableDestination destination, BigQueryException e) {
        String message = "Failed to " + operation + " BigQuery table " + destination;
        return isRetriable(e)
                ? new RetriableTableAdminException(message, e)
                : new IOException(message, e);
    }

    private static String quotedTable(TableDestination destination) {
        return "`"
                + escapeIdentifier(destination.getProject())
                + "."
                + escapeIdentifier(destination.getDataset())
                + "."
                + escapeIdentifier(destination.getTable())
                + "`";
    }

    private static String quotedOptionsView(TableDestination destination) {
        return "`"
                + escapeIdentifier(destination.getProject())
                + "."
                + escapeIdentifier(destination.getDataset())
                + ".INFORMATION_SCHEMA.TABLE_OPTIONS`";
    }

    private static String escapeIdentifier(String identifier) {
        return identifier.replace("\\", "\\\\").replace("`", "\\`");
    }

    /**
     * Types a failed creation for the caller: {@link RetriableTableAdminException} when repeating
     * the call can fix it, a plain {@link IOException} when it cannot.
     *
     * @param destination the table the creation was for
     * @param e the failure from the client
     * @return the failure to throw
     */
    @VisibleForTesting
    static IOException toFailure(TableDestination destination, BigQueryException e) {
        String message = "Failed to create BigQuery table " + destination;
        return isRetriable(e)
                ? new RetriableTableAdminException(message, e)
                : new IOException(message, e);
    }

    /**
     * Whether repeating a REST call that failed this way can succeed.
     *
     * <p>Three sources, and the client library is only the first of them. {@link
     * BigQueryException#isRetryable()} is the SDK's own verdict — server errors and the network
     * failures it wraps — and is borrowed rather than restated so a client release that widens it
     * widens this too. HTTP 429 is the standard rate-limit code. And the {@code rateLimitExceeded}
     * reason is BigQuery's per-table metadata-update quota, the same one {@link #isLostRace}
     * already names for schema updates: one constant, now two consumers, because creating and
     * updating a table spend the same budget.
     *
     * <p><b>The client does not retry any of this itself.</b> {@code BigQueryImpl.create} does run
     * under {@code runWithRetries}, but the handler it runs under consults {@code isRetryable()},
     * whose {@code RETRYABLE_ERRORS} set is {@code 500/502/503/504} alone (checked against
     * google-cloud-bigquery 2.68.0) — so a rate-limited creation surfaces to the caller on the
     * first attempt. Measured 2026-08-08 by racing sixteen concurrent creations of one missing
     * table: five answered {@code 403}, reason {@code rateLimitExceeded}, "Exceeded rate limits:
     * too many table update operations for this table", with {@code isRetryable()} reporting {@code
     * false}.
     *
     * <p>{@code quotaExceeded} is deliberately absent, on the widen-only-what-was-observed rule the
     * missing-table verdict was written under (ADR-0030): what the measurement answered was {@code
     * rateLimitExceeded}, and no creation here has been seen to answer the other reason. BigQuery
     * attaches it to quotas that refill on boundaries longer than any connector budget as well as
     * to rates, so accepting it unmeasured would risk turning a failure that is immediate and names
     * its own reason into a budget exhaustion that does not.
     *
     * @param e the failure from the client
     * @return whether the call may be repeated
     */
    @VisibleForTesting
    static boolean isRetriable(BigQueryException e) {
        if (e.isRetryable() || e.getCode() == HTTP_TOO_MANY_REQUESTS) {
            return true;
        }
        return REASON_RATE_LIMIT_EXCEEDED.equals(reasonOf(e));
    }

    @Override
    public TableSchemaSnapshot getSchema(TableDestination destination) throws IOException {
        Table table;
        try {
            table = client(destination).getTable(toTableId(destination));
        } catch (BigQueryException e) {
            throw new IOException("Failed to read the schema of BigQuery table " + destination, e);
        }
        if (table == null) {
            return null;
        }
        Schema schema = table.<TableDefinition>getDefinition().getSchema();
        if (schema == null) {
            throw new IOException("BigQuery table " + destination + " has no schema");
        }
        try {
            return TableSchemaSnapshot.of(BigQuerySchemaConverter.toStorageSchema(schema), table);
        } catch (RuntimeException e) {
            // For example a column type the converter does not support.
            throw new IOException(
                    "Failed to convert the schema of BigQuery table "
                            + destination
                            + " to its Storage API form",
                    e);
        }
    }

    @Override
    public boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed)
            throws IOException {
        Table baseTable = base.getTable();
        if (baseTable == null) {
            throw new IOException(
                    "The schema snapshot of BigQuery table "
                            + destination
                            + " carries no table resource to update");
        }
        TableDefinition definition = baseTable.getDefinition();
        if (!(definition instanceof StandardTableDefinition)) {
            throw new IOException(
                    "Cannot update the schema of BigQuery table "
                            + destination
                            + ": only standard tables are supported, found "
                            + definition.getType());
        }
        Schema existingSchema = definition.getSchema();
        Schema mergedSchema =
                existingSchema == null
                        ? StorageSchemaConverter.toBigQuerySchema(proposed)
                        : mergeSchema(existingSchema, proposed);
        StandardTableDefinition updated =
                ((StandardTableDefinition) definition).toBuilder().setSchema(mergedSchema).build();
        try {
            // The table carries the snapshot's etag, so BigQuery rejects the update when the
            // table changed since the snapshot was taken.
            client(destination).update(baseTable.toBuilder().setDefinition(updated).build());
            LOG.info("Updated the schema of BigQuery table {}", destination);
            return true;
        } catch (BigQueryException e) {
            if (isLostRace(e)) {
                LOG.info(
                        "A schema update of BigQuery table {} lost a race and will be retried"
                                + " from a fresh read (cause: {})",
                        destination,
                        e.toString());
                return false;
            }
            throw new IOException(
                    "Failed to update the schema of BigQuery table " + destination, e);
        }
    }

    /**
     * Merges a proposed Storage-form schema onto the existing REST schema it was derived from:
     * fields already present keep their REST {@code Field} — including attributes the Storage form
     * cannot represent, such as policy tags and collation — with only a {@code REQUIRED} → {@code
     * NULLABLE} relaxation applied when the proposal asks for it (and struct subfields merged
     * recursively); proposed-only fields are appended in Storage-converted form.
     */
    @VisibleForTesting
    static Schema mergeSchema(Schema existing, TableSchema proposed) {
        return Schema.of(mergeFields(existing.getFields(), proposed.getFieldsList()));
    }

    private static List<Field> mergeFields(
            FieldList existing, List<TableFieldSchema> proposedFields) {
        Map<String, Field> existingByName = new HashMap<>();
        for (Field field : existing) {
            existingByName.put(field.getName().toLowerCase(Locale.ROOT), field);
        }
        List<Field> merged = new ArrayList<>(proposedFields.size());
        for (TableFieldSchema proposed : proposedFields) {
            Field existingField = existingByName.get(proposed.getName().toLowerCase(Locale.ROOT));
            if (existingField == null) {
                merged.add(StorageSchemaConverter.toBigQueryField(proposed));
                continue;
            }
            Field.Builder builder = existingField.toBuilder();
            if (existingField.getMode() == Field.Mode.REQUIRED
                    && proposed.getMode() == TableFieldSchema.Mode.NULLABLE) {
                builder.setMode(Field.Mode.NULLABLE);
            }
            if (proposed.getType() == TableFieldSchema.Type.STRUCT
                    && existingField.getType() == LegacySQLTypeName.RECORD) {
                builder.setType(
                        LegacySQLTypeName.RECORD,
                        FieldList.of(
                                mergeFields(
                                        existingField.getSubFields(), proposed.getFieldsList())));
            }
            merged.add(builder.build());
        }
        return merged;
    }

    /**
     * Whether a schema-update failure means the update lost a race (concurrent change or metadata
     * quota) rather than being invalid: an etag-precondition failure, a conflict, or the per-table
     * metadata-update rate limit.
     */
    @VisibleForTesting
    static boolean isLostRace(BigQueryException e) {
        if (e.getCode() == HTTP_CONFLICT || e.getCode() == HTTP_PRECONDITION_FAILED) {
            return true;
        }
        String reason = reasonOf(e);
        return REASON_CONDITION_NOT_MET.equals(reason) || REASON_RATE_LIMIT_EXCEEDED.equals(reason);
    }

    /**
     * The error reason the service attached, preferring the structured error's over the exception's
     * own — the client populates one or the other depending on how the failure was constructed.
     */
    @Nullable
    private static String reasonOf(BigQueryException e) {
        return e.getError() != null ? e.getError().getReason() : e.getReason();
    }

    /**
     * Converts a destination to the REST client's table id.
     *
     * @param destination the destination
     * @return the table id
     */
    public static TableId toTableId(TableDestination destination) {
        return TableId.of(
                destination.getProject(), destination.getDataset(), destination.getTable());
    }

    /**
     * Converts creation options to the REST client's partitioning spec.
     *
     * @param options the creation options
     * @return the partitioning, or {@code null} when none is configured
     */
    private static TimePartitioning toTimePartitioning(TableCreateOptions options) {
        if (options.getTimePartitioningType() == null) {
            return null;
        }
        TimePartitioning.Builder partitioning =
                TimePartitioning.newBuilder(
                        TimePartitioning.Type.valueOf(options.getTimePartitioningType().name()));
        if (options.getTimePartitioningField() != null) {
            partitioning.setField(options.getTimePartitioningField());
        }
        if (options.getTimePartitioningExpirationMs() != null) {
            partitioning.setExpirationMs(options.getTimePartitioningExpirationMs());
        }
        return partitioning.build();
    }

    /**
     * Converts creation options to the REST client's clustering spec.
     *
     * @param options the creation options
     * @return the clustering, or {@code null} when none is configured
     */
    private static Clustering toClustering(TableCreateOptions options) {
        if (options.getClusteredFields().isEmpty()) {
            return null;
        }
        return Clustering.newBuilder().setFields(options.getClusteredFields()).build();
    }

    @VisibleForTesting
    static TableInfo buildTableInfo(
            TableDestination destination, TableSchema schema, TableCreateOptions options) {
        StandardTableDefinition.Builder definition =
                StandardTableDefinition.newBuilder()
                        .setSchema(StorageSchemaConverter.toBigQuerySchema(schema));
        TimePartitioning partitioning = toTimePartitioning(options);
        if (partitioning != null) {
            definition.setTimePartitioning(partitioning);
        }
        Clustering clustering = toClustering(options);
        if (clustering != null) {
            definition.setClustering(clustering);
        }
        return TableInfo.newBuilder(toTableId(destination), definition.build()).build();
    }

    @VisibleForTesting
    static TableInfo buildTableInfo(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptions createOptions,
            CdcTableOptions cdcOptions) {
        TableInfo.Builder table = buildTableInfo(destination, schema, createOptions).toBuilder();
        if (!cdcOptions.getPrimaryKeyColumns().isEmpty()) {
            table.setTableConstraints(
                    TableConstraints.newBuilder()
                            .setPrimaryKey(
                                    PrimaryKey.newBuilder()
                                            .setColumns(cdcOptions.getPrimaryKeyColumns())
                                            .build())
                            .build());
        }
        return table.build();
    }

    private BigQuery client(TableDestination destination) throws IOException {
        if (client == null) {
            if (emulatorEndpoint != null) {
                client = emulatorOptions(emulatorEndpoint, destination.getProject()).getService();
            } else if (serviceAccountKeyFile == null) {
                client = BigQueryOptions.getDefaultInstance().getService();
            } else {
                client = BigQueryCredentials.bigQueryOptions(serviceAccountKeyFile).getService();
            }
        }
        return client;
    }

    /**
     * Builds the options of a client talking to a BigQuery emulator with no credentials.
     *
     * <p>The emulator serves plain HTTP, and {@code setHost} takes a URL where the gRPC side takes
     * a bare {@code host:port} — hence the scheme here rather than in the configured value.
     *
     * <p>The project id is <em>required</em> rather than informative: {@link BigQueryOptions}
     * refuses to build without one it can determine, and an emulator offers no environment to
     * determine it from — so leaving it unset fails wherever no gcloud configuration exists, a CI
     * runner say, while passing on a developer's machine. Which project it is does not matter,
     * since every request made here names its table in full (see {@link #toTableId}); that is also
     * why one cached client stays correct across destinations in several projects.
     *
     * <p>Public because the source's query runner builds its REST client the same way, and a second
     * spelling of the paragraph above is a second thing to keep true. It reaches no published
     * surface: this class is {@code @Internal}.
     *
     * @param endpoint the emulator's REST endpoint
     * @param project the project id to satisfy the builder with
     * @return the options
     */
    public static BigQueryOptions emulatorOptions(EmulatorEndpoint endpoint, String project) {
        return BigQueryOptions.newBuilder()
                .setHost("http://" + endpoint.getTarget())
                .setProjectId(project)
                .setCredentials(NoCredentials.getInstance())
                .build();
    }
}
