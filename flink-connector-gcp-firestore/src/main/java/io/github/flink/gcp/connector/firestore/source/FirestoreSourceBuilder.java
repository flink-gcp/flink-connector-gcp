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

package io.github.flink.gcp.connector.firestore.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.FieldPath;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.batch.FirestoreBatchEnumeratorState;
import io.github.flink.gcp.connector.firestore.source.batch.FirestoreBatchSource;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.DefaultQueryPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.batch.reader.ClientQueryPageReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link FirestoreSource}.
 *
 * <p>Set the database, the deserializer, and exactly one of {@link #collectionGroup(String)} and
 * {@link #query(FirestoreQueryFactory)}. Everything else is optional.
 *
 * @param <T> the record type produced
 */
@PublicEvolving
public class FirestoreSourceBuilder<T> {

    /** The default number of documents one request asks for. */
    public static final int DEFAULT_PAGE_SIZE = 500;

    /** The characters a dot-separated field path cannot hold. */
    private static final String RESERVED = "~*/[]";

    private @Nullable DatabaseDestination database;
    private @Nullable FirestoreDocumentDeserializationSchema<T> deserializer;
    private @Nullable String collectionGroup;
    private @Nullable FirestoreQueryFactory queryFactory;
    private final List<String> fieldMask = new ArrayList<>();
    private @Nullable Integer partitionCount;
    private @Nullable Instant readTime;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private @Nullable String serviceAccountKeyFile;
    private @Nullable EmulatorEndpoint emulatorEndpoint;
    private @Nullable QueryPlannerFactory plannerFactory;
    private @Nullable QueryPageReader pageReader;

    FirestoreSourceBuilder() {}

    /**
     * Sets the database to read. Required.
     *
     * @param database the database
     * @return this builder
     */
    public FirestoreSourceBuilder<T> database(DatabaseDestination database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the deserializer turning documents into records. Required.
     *
     * @param deserializer the deserializer
     * @return this builder
     */
    public FirestoreSourceBuilder<T> deserializer(
            FirestoreDocumentDeserializationSchema<T> deserializer) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        return this;
    }

    /**
     * Reads every document of a collection group: every collection with this id, at any depth of
     * the database. The read is cut into partitions the service plans, read in parallel. Set this
     * or {@link #query(FirestoreQueryFactory)}, not both.
     *
     * <p>A partitioned read cannot filter or order: the service partitions only a whole collection
     * group, in document-name order. Use {@link #query(FirestoreQueryFactory)} to read a filtered
     * query, as one split.
     *
     * @param collectionGroup the collection id, for example {@code orders}
     * @return this builder
     * @throws IllegalArgumentException if the id is blank, has leading or trailing whitespace, or
     *     contains {@code '/'} — a collection id is one path segment
     */
    public FirestoreSourceBuilder<T> collectionGroup(String collectionGroup) {
        this.collectionGroup = ResourceNames.checkComponent(collectionGroup, "collectionGroup");
        return this;
    }

    /**
     * Reads one query, as one split. Set this or {@link #collectionGroup(String)}, not both.
     *
     * <p>The factory runs on the JobManager, with the client the source plans the read with, when
     * it plans the read; see {@link FirestoreQueryFactory} for what a query may carry.
     *
     * @param queryFactory builds the query from the source's client
     * @return this builder
     */
    public FirestoreSourceBuilder<T> query(FirestoreQueryFactory queryFactory) {
        this.queryFactory =
                Preconditions.checkNotNull(queryFactory, "queryFactory must not be null");
        return this;
    }

    /**
     * Reads only the given fields of each document of a collection-group scan. Optional; every
     * field is read when unset. Repeatable, adding to the fields already set.
     *
     * <p>Field paths are dot-separated, as the client library's {@code Query.select(String...)}
     * takes them: {@code a.b} is field {@code b} inside map {@code a}. Name a field whose name
     * contains a dot, or one of the characters {@code ~}, {@code *}, {@code /}, {@code [} and
     * {@code ]}, with {@link #select(FieldPath...)}. A query read through {@link
     * #query(FirestoreQueryFactory)} projects in the factory instead, with {@code select} on the
     * query it returns.
     *
     * @param fieldPaths the field paths to read
     * @return this builder
     * @throws IllegalArgumentException if a path is blank, holds an empty segment, or holds a
     *     character a dot-separated path cannot
     */
    public FirestoreSourceBuilder<T> select(String... fieldPaths) {
        Preconditions.checkNotNull(fieldPaths, "fieldPaths must not be null");
        for (String fieldPath : fieldPaths) {
            Preconditions.checkNotNull(fieldPath, "a field path must not be null");
            Preconditions.checkArgument(!fieldPath.isBlank(), "a field path must not be blank");
            // Parsed here rather than by the library's FieldPath.fromDotSeparatedString, which is
            // @InternalApi: the same reserved characters, and an empty segment, which the library
            // keeps and only the planner would refuse, naming the encoded path.
            Preconditions.checkArgument(
                    fieldPath.chars().noneMatch(c -> RESERVED.indexOf(c) >= 0),
                    "the field path '%s' holds one of ~, *, /, [ and ]; name such a field with"
                            + " select(FieldPath...)",
                    fieldPath);
            Preconditions.checkArgument(
                    !fieldPath.startsWith(".")
                            && !fieldPath.endsWith(".")
                            && !fieldPath.contains(".."),
                    "the field path '%s' has an empty segment",
                    fieldPath);
            fieldMask.add(FieldPath.of(fieldPath.split("\\.")).toString());
        }
        return this;
    }

    /**
     * Reads only the given fields of each document of a collection-group scan, each named by its
     * path segments, as the client library's {@code Query.select(FieldPath...)} takes them: {@code
     * FieldPath.of("a.b")} is one top-level field whose name contains a dot. Optional and
     * repeatable, like {@link #select(String...)}, which it adds to.
     *
     * @param fieldPaths the field paths to read
     * @return this builder
     */
    public FirestoreSourceBuilder<T> select(FieldPath... fieldPaths) {
        Preconditions.checkNotNull(fieldPaths, "fieldPaths must not be null");
        for (FieldPath fieldPath : fieldPaths) {
            Preconditions.checkNotNull(fieldPath, "a field path must not be null");
            // The encoded form, which FieldPath.fromServerFormat reads back: FieldPath is not
            // serializable, and the configuration travels with the job.
            fieldMask.add(fieldPath.toString());
        }
        return this;
    }

    /**
     * Sets how many partitions to ask the service for when scanning a collection group. Optional;
     * defaults to the source's parallelism.
     *
     * <p>It is an upper bound: the service answers with fewer partitions when the group is small. A
     * count above the parallelism lets a subtask that finishes early take another partition while a
     * slower one is still reading.
     *
     * @param partitionCount the positive number of partitions to ask for
     * @return this builder
     */
    public FirestoreSourceBuilder<T> partitionCount(int partitionCount) {
        Preconditions.checkArgument(
                partitionCount > 0, "partitionCount must be positive: %s", partitionCount);
        this.partitionCount = partitionCount;
        return this;
    }

    /**
     * Sets the snapshot time to read at. Optional; when unset, the source takes the service's
     * current time when it plans the read.
     *
     * <p>Every split reads at this one time. The service keeps old versions for an hour, or for
     * seven days with point-in-time recovery enabled, where a time older than an hour must fall on
     * a whole minute. A time outside that window fails the job when it plans the read — and the
     * window keeps moving while the job runs, so a read that outlasts it, or a restore from a
     * checkpoint older than it, fails too. The default is the service's time to the microsecond,
     * good for an hour: a read that may take longer needs point-in-time recovery and a whole-minute
     * time set here.
     *
     * @param readTime the snapshot time
     * @return this builder
     */
    public FirestoreSourceBuilder<T> readTime(Instant readTime) {
        this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        return this;
    }

    /**
     * Sets how many documents one request asks for. Optional; defaults to {@value
     * #DEFAULT_PAGE_SIZE}.
     *
     * <p>A request's documents are held in memory whole before any is handed on, so the page size
     * times the largest document is what one fetch ordinarily holds. A request whose stream breaks
     * is retried by the client library from its last document with the same limit, and the retry's
     * documents join the same result before the reader cuts it back, so each such retry can add up
     * to one page more. Lower it for large documents; raise it to make fewer requests for small
     * ones.
     *
     * @param pageSize the positive number of documents per request
     * @return this builder
     */
    public FirestoreSourceBuilder<T> pageSize(int pageSize) {
        Preconditions.checkArgument(pageSize > 0, "pageSize must be positive: %s", pageSize);
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Authenticates the source with the service-account JSON key at the given path instead of
     * application-default credentials. The JobManager reads the file when its enumerator is created
     * or restored, and each TaskManager reads it when its reader is created, so every process that
     * can run the source must see the same path. Optional; when unset the real-service path uses
     * application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public FirestoreSourceBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the source at a Firestore emulator instead of the real service. Optional; for tests.
     *
     * <p>The emulator needs no credentials, so setting this also stops the client from looking for
     * any. This setting is the only way the source reaches an emulator: the client library would
     * also read the {@code FIRESTORE_EMULATOR_HOST} environment variable on its own, and the source
     * logs a warning when that variable is set and this setting is not. The emulator does not
     * implement partitioning, so a collection-group scan against it needs a partition count of one.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public FirestoreSourceBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /** Replaces the factory the source mints the enumerator's planner from. For tests. */
    @VisibleForTesting
    FirestoreSourceBuilder<T> plannerFactory(QueryPlannerFactory plannerFactory) {
        this.plannerFactory = plannerFactory;
        return this;
    }

    /** Replaces the page reader the readers read through. For tests. */
    @VisibleForTesting
    FirestoreSourceBuilder<T> pageReader(QueryPageReader pageReader) {
        this.pageReader = pageReader;
        return this;
    }

    /**
     * Builds the source.
     *
     * @return the source
     * @throws IllegalStateException if a required option is missing, both or neither of {@code
     *     collectionGroup} and {@code query} are set, a projection or a partition count is set for
     *     a query, or both {@code serviceAccountKeyFile} and {@code emulatorEndpoint} are set
     */
    public Source<T, QuerySplit, FirestoreBatchEnumeratorState> build() {
        Preconditions.checkState(
                database != null, "A database is required. Set it with database(...).");
        Preconditions.checkState(
                deserializer != null, "A deserializer is required. Set it with deserializer(...).");
        Preconditions.checkState(
                (collectionGroup == null) != (queryFactory == null),
                "Set exactly one of collectionGroup(...) and query(...): a collection group is"
                        + " read as a partitioned scan, a query as one split.");
        Preconditions.checkState(
                fieldMask.isEmpty() || collectionGroup != null,
                "select(...) projects a collection-group scan. For a query, call select on the"
                        + " query the factory returns.");
        Preconditions.checkState(
                partitionCount == null || collectionGroup != null,
                "partitionCount(...) applies to a collection-group scan; a query is read as one"
                        + " split.");
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new FirestoreBatchSource<>(
                new FirestoreSourceConfig<>(
                        database,
                        deserializer,
                        collectionGroup,
                        queryFactory,
                        fieldMask,
                        partitionCount,
                        readTime,
                        pageSize,
                        serviceAccountKeyFile,
                        plannerFactory != null
                                ? plannerFactory
                                : new DefaultQueryPlannerFactory(emulatorEndpoint),
                        pageReader != null
                                ? pageReader
                                : new ClientQueryPageReader(emulatorEndpoint)));
    }
}
