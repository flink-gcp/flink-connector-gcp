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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;

import javax.annotation.Nullable;

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
@PublicEvolving
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

    private TableDestination table;
    private String parentProject;
    private BigQueryRowDeserializer<T> deserializer;
    private List<String> selectedFields = Collections.emptyList();
    @Nullable private String rowRestriction;
    @Nullable private Instant snapshotTime;
    private int maxStreamCount;
    private int preferredMinStreamCount;
    private int maxRecordsPerFetch = DEFAULT_MAX_RECORDS_PER_FETCH;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    @Nullable private ReadSessionCreator sessionCreator;
    @Nullable private RowStreamOpener rowStreamOpener;

    BigQuerySourceBuilder() {}

    /**
     * Sets the table to read.
     *
     * @param table the table
     * @return this builder
     */
    public BigQuerySourceBuilder<T> table(TableDestination table) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        return this;
    }

    /**
     * Sets the project the read session belongs to and is billed to.
     *
     * <p>Optional; defaults to the table's own project. Set it to read a table in another project —
     * a public dataset, say — where the read cannot be billed to the project that owns the table.
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
     * Sets the deserializer converting each row into a record.
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
     * Sends the source's traffic to a BigQuery emulator at {@code host:port}, over plaintext and
     * without credentials.
     *
     * <p>For testing against a local emulator and nothing else. Unlike the sink, the source takes
     * one endpoint rather than two: it reads the table's schema from the read session and makes no
     * REST call at all, so there is no second transport to point anywhere.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     */
    public BigQuerySourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
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

    /**
     * Builds the source.
     *
     * @return the source
     */
    public Source<T, BigQueryReadStreamSplit, BigQueryReadEnumeratorState> build() {
        Preconditions.checkState(table != null, "A table is required: set table(...).");
        Preconditions.checkState(deserializer != null, "A deserializer is required.");
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
                        parentProject == null ? table.getProject() : parentProject,
                        deserializer,
                        selectedFields,
                        rowRestriction,
                        snapshotTime,
                        maxStreamCount,
                        preferredMinStreamCount,
                        maxRecordsPerFetch,
                        sessionCreator == null
                                ? new ReadClientSessionCreator(emulatorEndpoint)
                                : sessionCreator,
                        rowStreamOpener == null
                                ? new ReadClientRowStreamOpener(emulatorEndpoint)
                                : rowStreamOpener));
    }
}
