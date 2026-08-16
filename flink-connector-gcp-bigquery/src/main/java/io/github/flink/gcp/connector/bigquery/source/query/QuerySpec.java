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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The query job a {@link QueryRunner} is asked to run.
 *
 * <p>A value object rather than four parameters, so the runner's one method keeps one argument as
 * the query path grows knobs.
 */
@Internal
public final class QuerySpec {

    private final String sql;
    private final String project;
    @Nullable private final String location;
    @Nullable private final String resultDataset;
    @Nullable private final QueryJobIdentity jobIdentity;

    /**
     * Creates the specification.
     *
     * @param sql the query to run
     * @param project the project the job is submitted to and billed to
     * @param location the BigQuery location the job runs in, or {@code null} to let BigQuery infer
     *     it from the tables the query names
     * @param resultDataset the dataset the result is written to, or {@code null} to let BigQuery
     *     write it into its own anonymous dataset and expire it
     */
    public QuerySpec(
            String sql, String project, @Nullable String location, @Nullable String resultDataset) {
        this(sql, project, location, resultDataset, null);
    }

    private QuerySpec(
            String sql,
            String project,
            @Nullable String location,
            @Nullable String resultDataset,
            @Nullable QueryJobIdentity jobIdentity) {
        this.sql = Preconditions.checkNotNull(sql, "sql must not be null");
        this.project = Preconditions.checkNotNull(project, "project must not be null");
        this.location = location;
        this.resultDataset = resultDataset;
        this.jobIdentity = jobIdentity;
    }

    /**
     * Returns a copy carrying the identity a reusable job is submitted and looked up under.
     *
     * <p>A wither rather than a constructor argument because the identity is derived <em>from</em>
     * the finished specification — its digest covers every other field — so it cannot exist before
     * the specification does.
     *
     * @param identity the identity
     * @return the copy
     */
    public QuerySpec withJobIdentity(QueryJobIdentity identity) {
        return new QuerySpec(
                sql,
                project,
                location,
                resultDataset,
                Preconditions.checkNotNull(identity, "identity must not be null"));
    }

    /**
     * Builds the specification that materializes a view.
     *
     * <p>This is the one query this connector <em>writes</em> rather than forwards, which is what
     * decides how the push-down knobs are treated. {@code selectedFields} is folded into the {@code
     * SELECT}, because a view's {@code SELECT *} scans every column and the query is billed for
     * what it scans — leaving the projection to the read session would prune the transfer after
     * paying for the scan. {@code rowRestriction} is <b>not</b> folded: it is BigQuery's
     * restriction syntax, which is not the same language as a SQL {@code WHERE}, so folding it
     * would give one knob two meanings depending on what the source was pointed at. It stays on the
     * read session, where a table source applies it too.
     *
     * @param view the view to materialize
     * @param selectedFields the columns to read, or empty for every column
     * @param project the project the job is submitted to and billed to
     * @param location the BigQuery location the job runs in, or {@code null} to let BigQuery infer
     *     it
     * @param resultDataset the dataset the result is written to, or {@code null} for BigQuery's
     *     anonymous one
     * @return the specification
     */
    public static QuerySpec forView(
            TableDestination view,
            List<String> selectedFields,
            String project,
            @Nullable String location,
            @Nullable String resultDataset) {
        Preconditions.checkNotNull(view, "view must not be null");
        Preconditions.checkNotNull(selectedFields, "selectedFields must not be null");
        String columns =
                selectedFields.isEmpty()
                        ? "*"
                        : selectedFields.stream()
                                .map(QuerySpec::quoteIdentifier)
                                .collect(Collectors.joining(", "));
        String sql =
                "SELECT "
                        + columns
                        + " FROM `"
                        + view.getProject()
                        + "."
                        + view.getDataset()
                        + "."
                        + view.getTable()
                        + "`";
        return new QuerySpec(sql, project, location, resultDataset);
    }

    /**
     * Quotes a column name for the generated {@code SELECT}.
     *
     * <p>A backtick inside an identifier is escaped rather than rejected: BigQuery allows one in a
     * flexible column name, and dropping it would build a query that names a different column.
     */
    private static String quoteIdentifier(String column) {
        return "`" + column.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /** Returns the query to run. */
    public String getSql() {
        return sql;
    }

    /** Returns the project the job is submitted to and billed to. */
    public String getProject() {
        return project;
    }

    /** Returns the location the job runs in, or {@code null} to let BigQuery infer it. */
    @Nullable
    public String getLocation() {
        return location;
    }

    /**
     * Returns the dataset the result is written to, or {@code null} for BigQuery's anonymous
     * dataset.
     */
    @Nullable
    public String getResultDataset() {
        return resultDataset;
    }

    /**
     * Returns the identity a reusable job is submitted and looked up under, or {@code null} for a
     * random id with no reuse — the default, and the fallback where no job name was readable.
     */
    @Nullable
    public QueryJobIdentity getJobIdentity() {
        return jobIdentity;
    }

    @Override
    public String toString() {
        return "QuerySpec{project="
                + project
                + ", location="
                + location
                + ", resultDataset="
                + (resultDataset == null ? "<anonymous>" : resultDataset)
                + ", sql="
                + sql
                + "}";
    }
}
