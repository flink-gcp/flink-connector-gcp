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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.query.BigQueryQueryRunner;
import io.github.flink.gcp.connector.bigquery.source.query.QueryRunner;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds a {@link BigQuerySource}.
 *
 * @param <T> type of the records produced by the source
 */
@Public
public class BigQuerySourceBuilder<T> {

    /**
     * The most rows one fetch hands to the task thread.
     *
     * <p>A BigQuery response block carries up to about 128 MiB of rows, so a cap is what lets a
     * checkpoint be taken part-way through one. The value follows the reference connector's own
     * default; together with Flink's {@code source.reader.element.queue.capacity} it bounds how
     * many decoded rows a subtask holds.
     */
    public static final int DEFAULT_MAX_RECORDS_PER_FETCH = 10_000;

    /**
     * Default for {@link #retryMaxAttempts(int)}: consecutive {@code ReadRows} attempts without
     * progress.
     *
     * <p>Read off the sequence it bounds rather than chosen for its own sake, and how long it is
     * depends on the failure. An {@code UNAVAILABLE} backs off exponentially — nominally 100 ms
     * growing by 1.3 — with gax picking each wait uniformly between zero and that value, which puts
     * twenty-five consecutive failures at about three minutes at worst and half that on average:
     * long enough to ride out the restarts a long read meets, short enough that a stream which is
     * never coming back is reported instead of retried for the SDK's own twenty-four hours. The
     * {@code INTERNAL} transport faults the client also resumes are retried a fixed millisecond
     * apart with no growth, so for those the bound is reached almost at once.
     */
    public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 25;

    @Nullable private TableDestination table;
    @Nullable private String query;
    @Nullable private String queryLocation;
    @Nullable private String queryResultDataset;
    @Nullable private Duration reuseQueryResultWithin;
    private boolean materializeViews;
    private String parentProject;
    private BigQueryRowDeserializer<T> deserializer;
    private List<String> selectedFields = Collections.emptyList();
    @Nullable private String rowRestriction;
    @Nullable private Instant snapshotTime;
    private int maxStreamCount;
    private int preferredMinStreamCount;
    private int maxRecordsPerFetch = DEFAULT_MAX_RECORDS_PER_FETCH;
    private int retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    @Nullable private EmulatorEndpoint emulatorRestEndpoint;
    @Nullable private ReadSessionCreator sessionCreator;
    @Nullable private RowStreamOpener rowStreamOpener;
    @Nullable private QueryRunner queryRunner;

    BigQuerySourceBuilder() {}

    /**
     * Sets the table to read.
     *
     * <p>Either this or {@link #query(String)}, never both and never neither. A <em>view</em> is
     * not a table here: the Storage Read API reads storage, and a view has none — pass its query,
     * or {@code SELECT * FROM the_view}, to {@link #query(String)} instead.
     *
     * @param table the table
     * @return this builder
     */
    public BigQuerySourceBuilder<T> table(TableDestination table) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        return this;
    }

    /**
     * Sets a query whose result is read, instead of reading a table directly.
     *
     * <p>The query is run once, at job start, as an ordinary BigQuery query job, and the source
     * reads the table its result landed in. That is what makes a <b>view readable</b> — the Storage
     * Read API cannot read a logical or materialized view at all, because it reads storage and a
     * view has none.
     *
     * <p><b>It is billed twice</b>: once for the bytes the query scans, and again for the bytes the
     * read session scans out of its result. Prune inside the query itself rather than relying on
     * {@link #selectedFields(String...)} and {@link #rowRestriction(String)}, which BigQuery
     * applies to the <em>result</em> and so cannot make the query cheaper.
     *
     * <p>Where the result lands is {@link #queryResultDataset(String)}'s choice, and by default it
     * is BigQuery's own anonymous dataset — nothing this connector has to create, expire or delete.
     *
     * <p>Requires {@link #parentProject(String)}: with no table named, nothing else says which
     * project the query job is submitted to and billed to.
     *
     * @param query the query, in GoogleSQL
     * @return this builder
     */
    public BigQuerySourceBuilder<T> query(String query) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(query), "query must not be blank");
        this.query = query;
        return this;
    }

    /**
     * Reads a {@link #table(TableDestination)} that turns out to be a view by materializing it.
     *
     * <p>Optional, and off by default. With it, the source asks BigQuery once, at job start, what
     * the configured name is; a view — logical or materialized — is then read the way {@link
     * #query(String)} reads one, by running {@code SELECT … FROM the_view} and reading its result.
     * An ordinary table is read directly, exactly as without this. Spark's and the Dataproc
     * connector's equivalent is spelled {@code viewsEnabled}.
     *
     * <p><b>Off by default because it costs a metadata call</b>, and a source pointed at a table
     * should not pay a round trip to be told it is a table — the read path otherwise makes no REST
     * call at all. Asking for this is also asking to be billed for a query nobody typed, which is
     * the other reason it is not the default.
     *
     * <p>{@link #selectedFields(String...)} is folded into the generated {@code SELECT}, so a view
     * is not scanned column by column for data that is then discarded. {@link
     * #rowRestriction(String)} is not: BigQuery's restriction syntax is not a SQL {@code WHERE}, so
     * it stays where a table source applies it, on the read session.
     *
     * <p>Where the materialized result lands, and what it costs, is {@link
     * #queryResultDataset(String)}'s choice, exactly as for {@link #query(String)}.
     *
     * @return this builder
     */
    public BigQuerySourceBuilder<T> materializeViews() {
        this.materializeViews = true;
        return this;
    }

    /**
     * Sets the BigQuery location the query job runs in.
     *
     * <p>Optional; defaults to letting BigQuery infer it from the tables the query names, which is
     * what it does for a query submitted without one. Set it where the inference has nothing to go
     * on, or where the job must be pinned to a region.
     *
     * @param queryLocation the location, for example {@code "US"} or {@code "asia-northeast1"}
     * @return this builder
     */
    public BigQuerySourceBuilder<T> queryLocation(String queryLocation) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(queryLocation),
                "queryLocation must not be blank");
        this.queryLocation = queryLocation;
        return this;
    }

    /**
     * Sets a dataset the query's result is written to, instead of BigQuery's anonymous dataset.
     *
     * <p>Optional, and the two choices differ in who owns the result:
     *
     * <ul>
     *   <li><b>Unset — BigQuery's anonymous dataset.</b> The query is submitted with no destination
     *       table, so BigQuery writes the result into a hidden dataset of its own, expires it after
     *       about a day and charges no storage for it. Nothing is created here, so nothing is left
     *       to clean up, and an identical query re-run within that window is answered from cache —
     *       free, and landing on the same table. Its constraints are BigQuery's: access is
     *       restricted to the identity that ran the query, Google advises against depending on a
     *       cached result table, and a result above the maximum response size is not kept.
     *   <li><b>Set — a table in this dataset.</b> The result is written to a table this connector
     *       creates there, with an expiration of a day set on it. Storage is charged for it until
     *       then, and nothing deletes it earlier: teardown also runs on a JobManager failover,
     *       where the restored job is still reading the read session that table backs.
     * </ul>
     *
     * <p>The dataset must already exist, must be in the query's own location, and must live in
     * {@link #parentProject(String)}.
     *
     * @param queryResultDataset the dataset id
     * @return this builder
     */
    public BigQuerySourceBuilder<T> queryResultDataset(String queryResultDataset) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(queryResultDataset),
                "queryResultDataset must not be blank");
        this.queryResultDataset = queryResultDataset;
        return this;
    }

    /**
     * Lets a re-planned job reuse a previous attempt's query job instead of running the query
     * again, for attempts within the given window.
     *
     * <p>Optional, and off by default — the job id is then random and every plan runs the query.
     * With it, the job id is derived from the <b>Flink job name</b>, a digest of the query
     * configuration, and the window, so a JobManager failover before the first checkpoint — the one
     * failure that re-plans a source — finds the first attempt's job and adopts it: a job still
     * running is waited for instead of racing it with a second scan, and a finished one has its
     * result table checked and read — a table that vanished meanwhile makes the attempt run the
     * query again instead. {@code queryJobsReattached} reports each reuse.
     *
     * <p><b>What it treats as "the same job" is the Flink job name</b>, because the name is the
     * identifier the user controls: rename the job and nothing is reused. The rest of the id is
     * derived — a digest over the query, project, location, result dataset and this window — so two
     * pipelines can only ever share a job when they would run the identical query to the identical
     * place, in which case sharing it is correct. The flip side is deliberate: attempts of the
     * <em>same</em> name and query inside one window reuse each other's result even across an
     * intentional redeploy, so the result can be up to a window old. Size the window to how stale a
     * result the pipeline can read, or rename the job to force a fresh one.
     *
     * <p>At most 24 hours, because both places a result can land expire at about a day: past that
     * there is nothing left to reuse — an adoption whose table has expired falls back to running
     * the query — so a longer window could only ever pay for the query again while appearing to
     * deduplicate it.
     *
     * <p><b>Requires {@link #queryLocation(String)}</b>: BigQuery scopes a job to (project,
     * location, id), and a look-up that names no location sees only the US multi-region — outside
     * it the previous attempt's job would never be found, so the reuse this knob asks for could
     * never happen (measured 2026-08-10 against a us-central1 dataset).
     *
     * @param reuseQueryResultWithin the window, positive and at most 24 hours
     * @return this builder
     */
    public BigQuerySourceBuilder<T> reuseQueryResultWithin(Duration reuseQueryResultWithin) {
        Preconditions.checkNotNull(
                reuseQueryResultWithin, "reuseQueryResultWithin must not be null");
        Preconditions.checkArgument(
                !reuseQueryResultWithin.isNegative() && !reuseQueryResultWithin.isZero(),
                "reuseQueryResultWithin must be positive: %s",
                reuseQueryResultWithin);
        Preconditions.checkArgument(
                reuseQueryResultWithin.compareTo(Duration.ofHours(24)) <= 0,
                "reuseQueryResultWithin must be at most 24 hours: %s. Both places a query result"
                        + " can land expire after about a day, so a longer window has nothing to"
                        + " reuse: every older adoption would find the table gone and run the"
                        + " query again.",
                reuseQueryResultWithin);
        this.reuseQueryResultWithin = reuseQueryResultWithin;
        return this;
    }

    /**
     * Sets the project the read session belongs to and is billed to.
     *
     * <p>Optional beside {@link #table(TableDestination)}, where it defaults to the table's own
     * project; set it to read a table in another project — a public dataset, say — where the read
     * cannot be billed to the project that owns the table. <b>Required beside {@link
     * #query(String)}</b>, which names no table to take a default from, and where it is also the
     * project the query job runs in and is billed to.
     *
     * @param parentProject the Google Cloud project id
     * @return this builder
     */
    public BigQuerySourceBuilder<T> parentProject(String parentProject) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(parentProject),
                "parentProject must not be blank");
        this.parentProject = parentProject;
        return this;
    }

    /**
     * Sets the deserializer converting each row into zero or more records.
     *
     * @param deserializer the deserializer
     * @return this builder
     */
    public BigQuerySourceBuilder<T> deserializer(BigQueryRowDeserializer<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Sets the columns to read.
     *
     * <p>Optional; defaults to every column. The projection is applied by BigQuery when the read
     * session is created, so unread columns are neither scanned nor billed.
     *
     * @param fields the column names
     * @return this builder
     */
    public BigQuerySourceBuilder<T> selectedFields(String... fields) {
        Preconditions.checkNotNull(fields, "fields must not be null");
        return selectedFields(Arrays.asList(fields));
    }

    /**
     * Sets the columns to read.
     *
     * @param fields the column names
     * @return this builder
     * @see #selectedFields(String...)
     */
    public BigQuerySourceBuilder<T> selectedFields(Collection<String> fields) {
        Preconditions.checkNotNull(fields, "fields must not be null");
        Set<String> distinct = new LinkedHashSet<>();
        for (String field : fields) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(field),
                    "a selected field must not be blank");
            Preconditions.checkArgument(
                    distinct.add(field), "selected field '%s' is named twice", field);
        }
        this.selectedFields = Collections.unmodifiableList(new ArrayList<>(distinct));
        return this;
    }

    /**
     * Sets a filter BigQuery applies before any row is sent.
     *
     * <p>Optional; defaults to no filter. The expression is BigQuery's own restriction syntax, a
     * {@code WHERE} clause without the keyword, for example {@code "state = 'CA' AND year > 2020"}.
     * Rows it excludes are neither transferred nor billed.
     *
     * @param rowRestriction the restriction
     * @return this builder
     */
    public BigQuerySourceBuilder<T> rowRestriction(String rowRestriction) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(rowRestriction),
                "rowRestriction must not be blank");
        this.rowRestriction = rowRestriction;
        return this;
    }

    /**
     * Sets the instant the table is read as of.
     *
     * <p>Optional; defaults to the table's contents when the read session is created. BigQuery
     * serves this from its time-travel window and rejects an instant outside it.
     *
     * @param snapshotTime the instant to read the table as of
     * @return this builder
     */
    public BigQuerySourceBuilder<T> snapshotTime(Instant snapshotTime) {
        this.snapshotTime =
                Preconditions.checkNotNull(snapshotTime, "snapshotTime must not be null");
        return this;
    }

    /**
     * Sets an upper bound on the number of read streams BigQuery creates.
     *
     * <p>Optional; defaults to {@code 0}, which lets BigQuery choose. It is a cap and not a target:
     * BigQuery returns at most this many streams and may return far fewer — a small table is read
     * by a single stream however many are asked for (measured 2026-08-09). Since one stream is read
     * by one subtask at a time, capping it below the job's parallelism leaves subtasks idle.
     *
     * @param maxStreamCount the upper bound, or {@code 0} to let BigQuery choose
     * @return this builder
     */
    public BigQuerySourceBuilder<T> maxStreamCount(int maxStreamCount) {
        Preconditions.checkArgument(
                maxStreamCount >= 0, "maxStreamCount must not be negative: %s", maxStreamCount);
        this.maxStreamCount = maxStreamCount;
        return this;
    }

    /**
     * Sets the number of read streams to ask BigQuery for.
     *
     * <p>Optional; defaults to {@code 0}, which asks for no particular number. BigQuery makes a
     * best effort to provide at least this many and may provide fewer. Asking for more streams than
     * there are subtasks is how this source gets its elasticity: readers take the next stream as
     * they finish one, so over-provisioning is what keeps a slow stream from holding a subtask
     * idle.
     *
     * @param preferredMinStreamCount the preferred lower bound, or {@code 0} for none
     * @return this builder
     */
    public BigQuerySourceBuilder<T> preferredMinStreamCount(int preferredMinStreamCount) {
        Preconditions.checkArgument(
                preferredMinStreamCount >= 0,
                "preferredMinStreamCount must not be negative: %s",
                preferredMinStreamCount);
        this.preferredMinStreamCount = preferredMinStreamCount;
        return this;
    }

    /**
     * Sets the most rows one fetch hands to the task thread.
     *
     * <p>Optional; defaults to {@link #DEFAULT_MAX_RECORDS_PER_FETCH}. A BigQuery response block
     * holds far more rows than this, so the cap is what lets a checkpoint be taken part-way through
     * one instead of after it.
     *
     * @param maxRecordsPerFetch the cap
     * @return this builder
     */
    public BigQuerySourceBuilder<T> maxRecordsPerFetch(int maxRecordsPerFetch) {
        Preconditions.checkArgument(
                maxRecordsPerFetch > 0,
                "maxRecordsPerFetch must be positive: %s",
                maxRecordsPerFetch);
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        return this;
    }

    /**
     * Sets how many consecutive {@code ReadRows} attempts without progress the client makes before
     * the read fails.
     *
     * <p>Optional; defaults to {@link #DEFAULT_RETRY_MAX_ATTEMPTS}. The retry itself is the client
     * library's, not this connector's: it resumes a broken stream at the row it had reached, and
     * decides for itself which failures deserve a resume. This knob only stops it. Without one the
     * client retries for twenty-four hours, so a stream that will never come back holds a reader
     * for a day while reporting nothing.
     *
     * <p>An attempt that produced rows resets the count, so this bounds a stream that is stuck
     * rather than one that is slow. Raise it for a read that must survive a long outage; the job
     * fails and restarts from its last checkpoint either way, resuming each stream at the offset
     * that checkpoint holds rather than reading it from the top.
     *
     * @param retryMaxAttempts the maximum number of consecutive attempts without progress
     * @return this builder
     */
    public BigQuerySourceBuilder<T> retryMaxAttempts(int retryMaxAttempts) {
        Preconditions.checkArgument(
                retryMaxAttempts > 0, "retryMaxAttempts must be positive: %s", retryMaxAttempts);
        this.retryMaxAttempts = retryMaxAttempts;
        return this;
    }

    /**
     * Uses the service account in the given JSON key file for every BigQuery client this source
     * opens.
     *
     * <p>Optional; absent uses application-default credentials. Only the path enters the job graph.
     * The source loads the file when its runtime clients are first opened: the JobManager creates
     * read sessions and runs query or view-materialization jobs, and TaskManagers open the assigned
     * read streams. The same key file must therefore exist at that path on both process types,
     * including after failover or rescaling.
     *
     * <p>Only service-account JSON is accepted. This setting cannot be combined with either
     * emulator endpoint because emulator connections are credential-free.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public BigQuerySourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Sends the source's traffic to a BigQuery emulator at {@code host:port}, over plaintext and
     * without credentials.
     *
     * <p>For testing against a local emulator and nothing else. This is the whole of it for a
     * source reading a {@link #table(TableDestination)} that does not ask for {@link
     * #materializeViews()}: the read session carries the schema, so nothing on that path makes a
     * REST call. A source reading a {@link #query(String)}, or one that asked for {@link
     * #materializeViews()}, does make one — the query job, and the view lookup — and needs {@link
     * #emulatorRestEndpoint(String)} as well.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public BigQuerySourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Sends the source's REST traffic — its query job, and the view lookup {@link
     * #materializeViews()} makes — to a BigQuery emulator at {@code host:port}, over plain HTTP and
     * without credentials.
     *
     * <p>This is the REST half of {@link #emulatorEndpoint(String)}. Two sources reach it: {@link
     * #query(String)}, whose query job is a REST call, and {@link #table(TableDestination)} with
     * {@link #materializeViews()}, whose view lookup is one. The lookup is made whether or not the
     * name turns out to be a view, so a {@code materializeViews()} source over an ordinary table
     * needs this endpoint and runs no query job at all. The two are separate because they are
     * separate transports on separate ports, as they are on the sink side.
     *
     * @param emulatorRestEndpoint the emulator's REST endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public BigQuerySourceBuilder<T> emulatorRestEndpoint(String emulatorRestEndpoint) {
        this.emulatorRestEndpoint =
                EmulatorEndpoint.parse(emulatorRestEndpoint, "emulatorRestEndpoint");
        return this;
    }

    @VisibleForTesting
    BigQuerySourceBuilder<T> sessionCreator(ReadSessionCreator sessionCreator) {
        this.sessionCreator = sessionCreator;
        return this;
    }

    @VisibleForTesting
    BigQuerySourceBuilder<T> rowStreamOpener(RowStreamOpener rowStreamOpener) {
        this.rowStreamOpener = rowStreamOpener;
        return this;
    }

    @VisibleForTesting
    BigQuerySourceBuilder<T> queryRunner(QueryRunner queryRunner) {
        this.queryRunner = queryRunner;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     */
    public Source<T, BigQueryReadStreamSplit, BigQueryReadEnumeratorState> build() {
        Preconditions.checkState(
                table != null || query != null,
                "A table or a query is required: set table(...) or query(...).");
        Preconditions.checkState(
                table == null || query == null,
                "table(...) and query(...) are alternatives: set one of them, not both.");
        Preconditions.checkState(deserializer != null, "A deserializer is required.");
        Preconditions.checkState(
                query == null || !materializeViews,
                "materializeViews() applies to table(...) only; query(...) already runs a query.");
        // Both kinds of source run a query job, so the knobs describing one apply to both.
        boolean runsAQuery = query != null || materializeViews;
        Preconditions.checkState(
                runsAQuery || queryLocation == null,
                "queryLocation(...) applies to query(...) or materializeViews() only; a table is"
                        + " read where it lives.");
        Preconditions.checkState(
                runsAQuery || queryResultDataset == null,
                "queryResultDataset(...) applies to query(...) or materializeViews() only; reading"
                        + " a table materializes nothing.");
        Preconditions.checkState(
                runsAQuery || reuseQueryResultWithin == null,
                "reuseQueryResultWithin(...) applies to query(...) or materializeViews() only; a"
                        + " table source runs no query job to reuse.");
        Preconditions.checkState(
                reuseQueryResultWithin == null || queryLocation != null,
                "reuseQueryResultWithin(...) requires queryLocation(...): BigQuery scopes a job to"
                        + " (project, location, id), and a look-up that names no location sees"
                        + " only the US multi-region — outside it a previous attempt's job would"
                        + " never be found, and the colliding resubmission fails instead of"
                        + " reusing (measured 2026-08-10 against a us-central1 dataset).");
        Preconditions.checkState(
                runsAQuery || emulatorRestEndpoint == null,
                "emulatorRestEndpoint(...) applies to query(...) or materializeViews() only; a"
                        + " table source makes no REST call, so there is nothing to point at an"
                        + " emulator.");
        Preconditions.checkState(
                serviceAccountKeyFile == null
                        || (emulatorEndpoint == null && emulatorRestEndpoint == null),
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...) or"
                        + " emulatorRestEndpoint(...); emulator connections are"
                        + " credential-free.");
        Preconditions.checkState(
                query == null || parentProject != null,
                "query(...) requires parentProject(...): it is the project the query job is"
                        + " submitted to and billed to, and no table names one.");
        Preconditions.checkState(
                query == null || snapshotTime == null,
                "snapshotTime(...) applies to table(...) only: a query's result table is created"
                        + " by the query, so there is no earlier version of it to read. Put the"
                        + " point in time in the query, as FOR SYSTEM_TIME AS OF.");
        Preconditions.checkState(
                !materializeViews || snapshotTime == null,
                "snapshotTime(...) and materializeViews() do not go together: if the name turns"
                        + " out to be a view, its result table is created now and has no earlier"
                        + " version, and the read would fail where the value was not typed. Drop"
                        + " one, or use query(...) with FOR SYSTEM_TIME AS OF.");
        Preconditions.checkState(
                maxStreamCount == 0
                        || preferredMinStreamCount == 0
                        || preferredMinStreamCount <= maxStreamCount,
                "preferredMinStreamCount must be at most maxStreamCount: %s > %s.",
                preferredMinStreamCount,
                maxStreamCount);
        return new BigQueryStorageReadSource<>(
                new BigQuerySourceConfig<>(
                        table,
                        query,
                        queryLocation,
                        queryResultDataset,
                        reuseQueryResultWithin,
                        materializeViews,
                        !runsAQuery
                                ? null
                                : (queryRunner == null
                                        ? new BigQueryQueryRunner(
                                                serviceAccountKeyFile, emulatorRestEndpoint)
                                        : queryRunner),
                        parentProject == null ? table.getProject() : parentProject,
                        deserializer,
                        selectedFields,
                        rowRestriction,
                        snapshotTime,
                        maxStreamCount,
                        preferredMinStreamCount,
                        maxRecordsPerFetch,
                        sessionCreator == null
                                ? new ReadClientSessionCreator(
                                        serviceAccountKeyFile, emulatorEndpoint)
                                : sessionCreator,
                        rowStreamOpener == null
                                ? new ReadClientRowStreamOpener(
                                        serviceAccountKeyFile, emulatorEndpoint, retryMaxAttempts)
                                : rowStreamOpener));
    }
}
