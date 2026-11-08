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

package io.github.flink.gcp.connector.testutils.bigquery;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * REST plumbing shared by the ITCases gated on real-GCP credentials: the {@code BQ_IT_*} variables,
 * a REST client over application-default credentials, table creation/deletion and query read-back.
 * The gated classes reach BigQuery through this and nothing else (#292); {@link RealGcs} is the
 * sibling for the FILE_LOADS staging bucket, which only that write method needs. Here rather than
 * in the connector's test tree so the Tier-3 BigQuery application's gated test shares it; the two
 * helpers that take connector types stay with the connector's tests.
 *
 * <p>Deliberately a static utility and <b>not</b> a base class carrying the gating annotation:
 * {@code scripts/e2e-gated-its.sh} parses test sources for the environment-variable gate and
 * demands a surefire report per discovered file, so an abstract class carrying it would fail {@code
 * --assert-ran}. Each gated ITCase declares its own {@code @EnabledIfEnvironmentVariable}
 * annotations.
 */
@Internal
public final class RealBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(RealBigQuery.class);

    private RealBigQuery() {}

    /** The GCP project the gated ITCases run against ({@code BQ_IT_PROJECT}). */
    public static String project() {
        return System.getenv("BQ_IT_PROJECT");
    }

    /** The BigQuery dataset the gated ITCases create their tables in ({@code BQ_IT_DATASET}). */
    public static String dataset() {
        return System.getenv("BQ_IT_DATASET");
    }

    /**
     * A REST client over application-default credentials. Private because every gated ITCase now
     * reads through the methods below (#292); a case they do not cover earns a method here rather
     * than a second client at the call site.
     */
    private static BigQuery client() {
        return BigQueryOptions.newBuilder().setProjectId(project()).build().getService();
    }

    /**
     * The gated dataset's location, which a reuse-enabled query source must name: BigQuery scopes a
     * job to (project, location, id), and a location-less {@code jobs.get} sees only the US
     * multi-region — measured 2026-08-10 against this suite's regional dataset.
     */
    public static String datasetLocation() {
        return client().getDataset(dataset()).getLocation();
    }

    /**
     * Creates {@code table} in the gated dataset with the given REST schema, which can express
     * columns the Storage Write API schema cannot, such as {@code INTERVAL}. The connector's tests
     * create one from a Storage Write API schema through their own {@code RealTables}.
     */
    public static void createTable(String table, Schema schema) {
        client().create(
                        TableInfo.of(
                                TableId.of(project(), dataset(), table),
                                StandardTableDefinition.of(schema)));
    }

    /** Creates a table carrying representative metadata that a data-only replacement must keep. */
    public static void createTableWithMetadata(
            String table,
            Schema schema,
            String description,
            Map<String, String> labels,
            TableConstraints constraints) {
        client().create(
                        TableInfo.newBuilder(
                                        TableId.of(project(), dataset(), table),
                                        StandardTableDefinition.of(schema))
                                .setDescription(description)
                                .setLabels(labels)
                                .setTableConstraints(constraints)
                                .build());
    }

    /**
     * Attempts every given table deletion and reports failures; the dataset's default table
     * expiration (24 h, set in {@code opentofu/flink-gcp/it-resources.tf}) is the backstop for a
     * crashed run. A short-term table-update rate limit gets at most six attempts, with waits of
     * one, two, four, eight and sixteen seconds. Interruption stops further cleanup.
     */
    public static void deleteTables(String... tables) {
        BigQuery client = client();
        deleteTables(
                table -> client.delete(TableId.of(project(), dataset(), table)),
                Thread::sleep,
                tables);
    }

    static void deleteTables(Consumer<String> delete, Sleeper sleeper, String... tables) {
        RuntimeException failure = null;
        for (String table : tables) {
            try {
                deleteTable(delete, sleeper, table);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                IllegalStateException aborted =
                        new IllegalStateException(
                                "Interrupted while deleting BigQuery E2E table " + table,
                                interrupted);
                if (failure != null) {
                    aborted.addSuppressed(failure);
                }
                throw aborted;
            } catch (RuntimeException error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void deleteTable(Consumer<String> delete, Sleeper sleeper, String table)
            throws InterruptedException {
        BigQueryException lastRateLimit = null;
        for (int attempt = 0; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                InterruptedException interrupted =
                        new InterruptedException("BigQuery E2E cleanup was interrupted");
                if (lastRateLimit != null) {
                    interrupted.addSuppressed(lastRateLimit);
                }
                throw interrupted;
            }
            try {
                delete.accept(table);
                return;
            } catch (BigQueryException error) {
                // The REST SDK does not retry the observed 403 metadata-update rate limit.
                if (error.getCode() != 403
                        || !"rateLimitExceeded".equals(error.getReason())
                        || attempt == 5) {
                    throw error;
                }
                lastRateLimit = error;
                try {
                    long delayMillis = 1000L << attempt;
                    LOG.warn(
                            "Rate-limited deleting BigQuery E2E table {} (attempt {}/6); retrying in {} ms",
                            table,
                            attempt + 1,
                            delayMillis);
                    sleeper.sleep(delayMillis);
                } catch (InterruptedException interrupted) {
                    interrupted.addSuppressed(error);
                    throw interrupted;
                }
            }
        }
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** Returns the live columns of {@code table}, in order. */
    public static FieldList tableFields(String table) {
        Schema schema = tableDefinition(table).getSchema();
        assertThat(schema).isNotNull();
        return schema.getFields();
    }

    /**
     * Returns the live definition of {@code table} — the schema, and the partitioning and
     * clustering the service actually stored.
     */
    public static StandardTableDefinition tableDefinition(String table) {
        return tableInfo(table).getDefinition();
    }

    /** Returns the table-level description of {@code table}. */
    public static String tableDescription(String table) {
        return tableInfo(table).getDescription();
    }

    /** Returns the labels stored on {@code table}. */
    public static Map<String, String> tableLabels(String table) {
        return tableInfo(table).getLabels();
    }

    /**
     * Returns the Cloud KMS key that encrypts {@code table}, or {@code null} when a Google-owned
     * key does.
     */
    @Nullable
    public static String tableKmsKeyName(String table) {
        return kmsKeyNameOf(tableInfo(table));
    }

    /**
     * Returns the Cloud KMS key that encrypts the result table of the one query job created since
     * {@code sinceMillis} whose SQL contains {@code marker}, or {@code null} when a Google-owned
     * key does. The job is found through {@code jobs.list}, which, unlike {@code
     * INFORMATION_SCHEMA.JOBS}, lists a job as soon as it exists.
     */
    @Nullable
    public static String queryResultKmsKeyName(String marker, long sinceMillis) {
        List<TableId> results = new ArrayList<>();
        for (com.google.cloud.bigquery.Job job :
                client().listJobs(
                                BigQuery.JobListOption.minCreationTime(sinceMillis),
                                BigQuery.JobListOption.stateFilter(
                                        com.google.cloud.bigquery.JobStatus.State.DONE))
                        .iterateAll()) {
            if (job.getConfiguration() instanceof QueryJobConfiguration) {
                QueryJobConfiguration query = job.getConfiguration();
                if (query.getQuery().contains(marker) && query.getDestinationTable() != null) {
                    results.add(query.getDestinationTable());
                }
            }
        }
        assertThat(results).as("query jobs whose SQL contains %s", marker).hasSize(1);
        Table result = client().getTable(results.get(0));
        assertThat(result).as("result table %s exists", results.get(0)).isNotNull();
        return kmsKeyNameOf(result);
    }

    @Nullable
    private static String kmsKeyNameOf(Table table) {
        return table.getEncryptionConfiguration() == null
                ? null
                : table.getEncryptionConfiguration().getKmsKeyName();
    }

    /** Returns the constraints stored on {@code table}. */
    public static TableConstraints tableConstraints(String table) {
        return tableInfo(table).getTableConstraints();
    }

    /** Returns the bytes processed by a completed query job in the gated dataset's location. */
    public static long queryBytesProcessed(String jobId) {
        com.google.cloud.bigquery.Job job =
                client().getJob(
                                com.google.cloud.bigquery.JobId.newBuilder()
                                        .setJob(jobId)
                                        .setLocation(datasetLocation())
                                        .build());
        assertThat(job).as("query job %s exists", jobId).isNotNull();
        com.google.cloud.bigquery.JobStatistics.QueryStatistics statistics = job.getStatistics();
        assertThat(statistics).as("query job %s has query statistics", jobId).isNotNull();
        return statistics.getTotalBytesProcessed();
    }

    private static Table tableInfo(String table) {
        Table live = client().getTable(TableId.of(project(), dataset(), table));
        // Named rather than left to a bare NPE below: a table the sink was expected to auto-create
        // is exactly what a caller of this asserts about, and the run that finds out is a paid one.
        assertThat(live).as("table %s exists", table).isNotNull();
        return live;
    }

    /** Runs the query and returns its rows. */
    public static List<FieldValueList> queryRows(String sql) throws InterruptedException {
        List<FieldValueList> rows = new ArrayList<>();
        client().query(QueryJobConfiguration.newBuilder(sql).build())
                .iterateAll()
                .forEach(rows::add);
        return rows;
    }

    /**
     * Runs the query and returns its first column, one entry per row, {@code null} for a NULL cell.
     *
     * <p>The null mapping is not decoration: {@link
     * com.google.cloud.bigquery.FieldValue#getLongValue()} is {@code
     * Long.parseLong(getStringValue())} and {@code getStringValue()} null-checks, so a NULL cell
     * throws — and defaulting it to {@code 0} would be a wrong answer rather than a failure.
     */
    public static List<Long> queryLongs(String sql) throws InterruptedException {
        List<Long> values = new ArrayList<>();
        for (FieldValueList row : queryRows(sql)) {
            values.add(row.get(0).isNull() ? null : row.get(0).getLongValue());
        }
        return values;
    }

    /** Returns the fully qualified, backtick-quoted table path for use in a query. */
    public static String tablePath(String table) {
        return "`" + project() + "." + dataset() + "." + table + "`";
    }
}
