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

import org.apache.flink.api.connector.source.Source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.QueryPartition;
import com.google.cloud.firestore.TransactionOptions;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreRealGcpITCase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Native-mode source against the real service: what {@code FirestoreSourceEmulatorITCase}
 * cannot show, because the emulator does not implement {@code PartitionQuery} and answers read
 * times the service refuses. The read-time and timestamp checks at the end measure the premises the
 * builder's microsecond truncation rests on.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class FirestoreSourceRealGcpITCase extends AbstractFirestoreRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSourceRealGcpITCase.class);

    /** Enough documents across two depths for the service to cut a collection group at all. */
    private static final int LARGE_GROUP = 3000;

    @Test
    void aScanReadsEveryDocumentOnceAcrossTheServicesPartitions() throws Exception {
        String group = uniqueCollection();
        List<String> paths = seed(group, LARGE_GROUP);
        List<QueryPartition> partitions =
                client().collectionGroup(group).getPartitions(8).get(60, TimeUnit.SECONDS);
        LOG.info("{} documents answered {} of 8 partitions", LARGE_GROUP, partitions.size());

        // More than one, or the scan below would be a single split and show nothing of the
        // service's partitioning; at most the count asked for.
        assertThat(partitions).hasSizeBetween(2, 8);
        assertThat(
                        read(
                                builder ->
                                        builder.collectionGroup(group)
                                                .partitionCount(8)
                                                .pageSize(500),
                                3))
                .containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void aSmallGroupComesBackInFewerPartitionsThanAsked() throws Exception {
        String group = uniqueCollection();
        List<String> paths = seed(group, 5);

        assertThat(client().collectionGroup(group).getPartitions(8).get(60, TimeUnit.SECONDS))
                .hasSizeLessThan(8);
        assertThat(read(builder -> builder.collectionGroup(group).partitionCount(8), 2))
                .containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void readsNothingFromAnEmptyGroupAtTheServicesReadTime() throws Exception {
        // The default read time comes from a one-document probe, which must carry a read time even
        // when it matches nothing: the planner fails the job if it does not.
        assertThat(read(builder -> builder.collectionGroup(uniqueCollection()), 1)).isEmpty();
    }

    @Test
    void aReadTimeBeforeTheDatabaseExistedFailsPlanning() throws Exception {
        // Without point-in-time recovery the service keeps versions for one hour, but a database
        // created minutes ago refuses an older read time for predating it, so the one-hour bound
        // itself cannot be reached here; it was measured once on databases more than an hour old.
        String group = uniqueCollection();
        seed(group, 1);
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));

        assertThatThrownBy(
                        () ->
                                read(
                                        builder ->
                                                builder.collectionGroup(group)
                                                        .readTime(twoHoursAgo),
                                        1))
                .hasStackTraceContaining("Failed to plan the Firestore read")
                .hasStackTraceContaining(
                        "INVALID_ARGUMENT: The requested 'read_time' cannot be before database"
                                + " creation time");
    }

    @Test
    void aReadTimeInTheFutureFailsPlanning() throws Exception {
        String group = uniqueCollection();
        seed(group, 1);
        Instant inAnHour = Instant.now().plus(Duration.ofHours(1));

        assertThatThrownBy(
                        () -> read(builder -> builder.collectionGroup(group).readTime(inAnHour), 1))
                .hasStackTraceContaining("Failed to plan the Firestore read")
                .hasStackTraceContaining(
                        "INVALID_ARGUMENT: The requested 'read_time' cannot be in the future");
    }

    @Test
    void readsALimitToLastQueryAndItsOffsetInReverse() throws Exception {
        String collection = uniqueCollection();
        seed(collection, 10);
        FirestoreQueryFactory factory =
                firestore -> firestore.collection(collection).orderBy("n").limitToLast(3).offset(2);
        List<String> expected =
                factory.create(client()).get().get(30, TimeUnit.SECONDS).getDocuments().stream()
                        .map(document -> document.getReference().getPath())
                        .collect(Collectors.toList());
        assertThat(expected).hasSize(3);
        Collections.reverse(expected);

        assertThat(read(builder -> builder.query(factory).pageSize(2), 2))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void aReadTimeFinerThanAMicrosecondIsRefused() throws Exception {
        // Sent through a read-only transaction, which passes the protobuf read time unchanged: the
        // builders truncate a configured read time, so the source never sends one.
        DocumentReference document = client().document(uniqueCollection() + "/a");
        Instant written = updateTime(document.set(Map.of("v", 1)).get(30, TimeUnit.SECONDS));

        // The same instant on a whole microsecond is answered, so the refusal is the precision's.
        assertThat(readAt(document, written).exists()).isTrue();
        assertThatThrownBy(() -> readAt(document, written.plusNanos(1)))
                .isInstanceOf(ExecutionException.class)
                .satisfies(
                        failure ->
                                assertThat(status(failure.getCause()))
                                        .isEqualTo(StatusCode.Code.INVALID_ARGUMENT))
                .hasStackTraceContaining("timestamp cannot have more than microseconds precision");
    }

    @Test
    void versionsSitOnWholeMicroseconds() throws Exception {
        // The premise behind "the truncated read time reads the same data": a version's time is a
        // whole microsecond, so no write falls between a sub-microsecond read time and its floor.
        DocumentReference document = client().document(uniqueCollection() + "/a");
        Instant written = updateTime(document.set(Map.of("v", 1)).get(30, TimeUnit.SECONDS));

        assertThat(written.getNano() % 1_000).isZero();
        assertThat(readAt(document, written).exists()).isTrue();
        assertThat(readAt(document, written.minusNanos(1_000)).exists()).isFalse();
    }

    @Test
    void aTimestampValueIsFlooredToTheMicrosecondButAFilterValueIsNot() throws Exception {
        // Documented as "precise only to microseconds; any additional precision is rounded down".
        // An equality filter carrying the unfloored value matches nothing, as on the emulator, so a
        // lookup by a timestamp finer than a microsecond misses the document it wrote.
        DocumentReference document = client().document(uniqueCollection() + "/a");
        Timestamp fine = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_789);
        Timestamp floored = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);
        document.set(Map.of("t", fine)).get(30, TimeUnit.SECONDS);

        assertThat(read(document.getPath()).getTimestamp("t")).isEqualTo(floored);
        assertThat(matches(document, fine)).isZero();
        assertThat(matches(document, floored)).isOne();
    }

    private static int matches(DocumentReference document, Timestamp value) throws Exception {
        return document.getParent().whereEqualTo("t", value).get().get(30, TimeUnit.SECONDS).size();
    }

    private static Instant updateTime(WriteResult result) {
        Timestamp time = result.getUpdateTime();
        return Instant.ofEpochSecond(time.getSeconds(), time.getNanos());
    }

    /** Reads the document in a read-only transaction at the instant, sent unchanged. */
    private static DocumentSnapshot readAt(DocumentReference document, Instant readTime)
            throws Exception {
        return client().runTransaction(
                        transaction -> transaction.get(document).get(),
                        TransactionOptions.createReadOnlyOptionsBuilder()
                                .setReadTime(
                                        com.google.protobuf.Timestamp.newBuilder()
                                                .setSeconds(readTime.getEpochSecond())
                                                .setNanos(readTime.getNano()))
                                .build())
                .get(30, TimeUnit.SECONDS);
    }

    private static StatusCode.Code status(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ApiException) {
                return ((ApiException) cause).getStatusCode().getCode();
            }
        }
        throw new AssertionError("no status in the chain", failure);
    }

    private static List<String> read(
            UnaryOperator<FirestoreSourceBuilder<String>> customizer, int parallelism)
            throws Exception {
        Source<String, ?, ?> source =
                customizer
                        .apply(
                                FirestoreSource.<String>builder()
                                        .database(database())
                                        .deserializer(new TestSources.DocumentPathDeserializer()))
                        .build();
        return TestSources.collect(source, parallelism);
    }

    /**
     * Writes {@code count} documents of the collection group {@code group}, half at the top level
     * and half under parent documents, each with {@code n}, and returns their paths.
     */
    private static List<String> seed(String group, int count) throws Exception {
        List<String> paths = new ArrayList<>();
        WriteBatch batch = client().batch();
        for (int n = 0; n < count; n++) {
            String path =
                    n % 2 == 0
                            ? group + "/d" + String.format("%05d", n)
                            : "parents/p" + (n % 7) + "/" + group + "/d" + String.format("%05d", n);
            batch.set(client().document(path), Map.of("n", n));
            paths.add(path);
            if (batch.getMutationsSize() == 500) {
                batch.commit().get(60, TimeUnit.SECONDS);
                batch = client().batch();
            }
        }
        if (batch.getMutationsSize() > 0) {
            batch.commit().get(60, TimeUnit.SECONDS);
        }
        return paths;
    }
}
