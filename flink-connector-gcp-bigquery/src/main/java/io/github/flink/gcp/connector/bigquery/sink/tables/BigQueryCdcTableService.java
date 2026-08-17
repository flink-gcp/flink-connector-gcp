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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.http.HttpTransportOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The {@link CdcTableProvisioner.Service} half of BigQuery table administration: the protocols the
 * CDC table contract needs and ordinary table administration never does.
 *
 * <p><b>Why this is not {@link BigQueryTableAdmin}.</b> That class speaks one protocol — the
 * BigQuery REST client, through {@code com.google.cloud.bigquery} — and this one speaks three more:
 * a hand-rolled conditional HTTP {@code PATCH} issued through the client's transport with a
 * method-override header, because the client library exposes no ETag precondition; two GoogleSQL
 * statements, because {@code max_staleness} is reachable only through DDL and {@code
 * INFORMATION_SCHEMA}; and the query-job protocol needed to run them. Each carries its own
 * retriability verdict, over failure types the REST half never sees.
 *
 * <p>The two share the client, which is why this takes a {@link ClientSupplier} rather than
 * building one: an emulator endpoint, a service-account key and the lazy first-use construction are
 * all {@link BigQueryTableAdmin}'s, and duplicating that would be a second place for a credential
 * decision to be made differently.
 *
 * <p>Per ADR-0071 the retriability verdicts and {@link RetriableTableAdminException} stay in this
 * package, beside the client and travelling as a type; the ADR pins the package rather than the
 * class, which is what lets them live here.
 */
@Internal
class BigQueryCdcTableService implements CdcTableProvisioner.Service {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCdcTableService.class);

    /**
     * Supplies the BigQuery client for a destination.
     *
     * <p>A supplier rather than the client itself: {@link BigQueryTableAdmin} builds it lazily on
     * first use, so that a job whose destinations are all reachable without one never constructs a
     * client, and a credential failure surfaces from the operation that needed it.
     */
    @FunctionalInterface
    interface ClientSupplier {
        BigQuery get(TableDestination destination) throws IOException;
    }

    /** Google API method override header used because the default JDK transport rejects PATCH. */
    private static final String METHOD_OVERRIDE_HEADER = "X-HTTP-Method-Override";

    private static final Pattern RATE_LIMIT_REASON =
            Pattern.compile("\\\"reason\\\"\\s*:\\s*\\\"rateLimitExceeded\\\"");

    private static final Set<String> RETRIABLE_JOB_REASONS =
            Set.of(
                    BigQueryTableAdmin.REASON_RATE_LIMIT_EXCEEDED,
                    "jobRateLimitExceeded",
                    "backendError",
                    "internalError",
                    "jobBackendError",
                    "jobInternalError");

    private final ClientSupplier clientSupplier;

    /** The query-job location, which only the GoogleSQL half of the contract needs. */
    @Nullable private final String location;

    BigQueryCdcTableService(ClientSupplier clientSupplier, @Nullable String location) {
        this.clientSupplier =
                Preconditions.checkNotNull(clientSupplier, "clientSupplier must not be null");
        this.location = location;
    }

    private BigQuery client(TableDestination destination) throws IOException {
        return clientSupplier.get(destination);
    }

    @Override
    @Nullable
    public CdcTableProvisioner.TableState read(TableDestination destination) throws IOException {
        Table table;
        try {
            table = client(destination).getTable(BigQueryTableAdmin.toTableId(destination));
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
                BigQueryTableAdmin.buildTableInfo(destination, schema, createOptions, cdcOptions)
                        .toBuilder()
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
            if (e.getCode() == BigQueryTableAdmin.HTTP_CONFLICT) {
                LOG.info("BigQuery CDC table {} already exists", destination);
                return false;
            }
            throw BigQueryTableAdmin.toFailure(destination, e);
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
            table = client(destination).getTable(BigQueryTableAdmin.toTableId(destination));
        } catch (BigQueryException e) {
            if (e.getCode() == BigQueryTableAdmin.HTTP_NOT_FOUND) {
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
            if (e.getStatusCode() == BigQueryTableAdmin.HTTP_CONFLICT
                    || e.getStatusCode() == BigQueryTableAdmin.HTTP_PRECONDITION_FAILED
                    || e.getStatusCode() == BigQueryTableAdmin.HTTP_NOT_FOUND) {
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
        return status == BigQueryTableAdmin.HTTP_TOO_MANY_REQUESTS
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
        return BigQueryTableAdmin.isRetriable(e)
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
        return BigQueryTableAdmin.isRetriable(e)
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
}
