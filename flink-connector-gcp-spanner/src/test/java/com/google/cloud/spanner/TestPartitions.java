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

package com.google.cloud.spanner;

import com.google.cloud.Timestamp;
import com.google.protobuf.ByteString;

import java.util.Arrays;
import java.util.List;

/**
 * Mints the two values a Spanner batch-read split carries, for tests that must not run a read.
 *
 * <p><b>Why this file declares a Google package.</b> {@link Partition} and {@link
 * BatchTransactionId} are produced only by a live {@code BatchReadOnlyTransaction}: neither has a
 * public constructor, a public factory, or a reachable super-constructor, and {@code Partition}
 * exposes nothing but its token. A test of the split serializer, of the enumerator's plan or of the
 * split reader genuinely reads those values, so there is no version of these tests that works
 * against a substitute. The alternative — a production type that hid the vendor's behind an
 * interface — would move test-only structure into the connector, and mocking is not used in this
 * repository.
 *
 * <p>This is the <b>second</b> source in this repository whose package is outside {@code
 * io.github.flink.gcp.*}; the first is the BigQuery module's {@code TestJobs}. A third is a
 * decision to take on its own evidence, not a precedent this one establishes.
 *
 * <p><b>What it reaches, and the risk that buys.</b> Three package-private members, verified
 * against {@code google-cloud-spanner} 6.119.0 on 2026-08-10:
 *
 * <ul>
 *   <li>{@code Partition.createQueryPartition(ByteString, PartitionOptions, Statement, Options)}
 *   <li>{@code Partition.createReadPartition(ByteString, PartitionOptions, String, String, KeySet,
 *       Iterable, Options)}
 *   <li>{@code BatchTransactionId(String, ByteString, Timestamp)}
 * </ul>
 *
 * <p>{@code Options.fromQueryOptions()} and {@code Options.fromReadOptions()} are reached too, and
 * only to produce the empty options object those factories require — the alternative, passing
 * {@code null}, would exercise a shape the client library never produces. A release that moves any
 * of them fails this file at compile time, which is the whole safety argument for reaching them
 * here rather than through reflection.
 */
public final class TestPartitions {

    private TestPartitions() {}

    /**
     * Returns a query partition carrying the given token.
     *
     * @param token the partition token, which need only be distinct between partitions
     * @param sql the query the partition belongs to
     * @return the partition
     */
    public static Partition queryPartition(String token, String sql) {
        return Partition.createQueryPartition(
                ByteString.copyFromUtf8(token),
                PartitionOptions.getDefaultInstance(),
                Statement.of(sql),
                Options.fromQueryOptions());
    }

    /**
     * Returns a table-read partition carrying the given token.
     *
     * @param token the partition token, which need only be distinct between partitions
     * @param table the table the partition reads
     * @param columns the columns the partition returns
     * @return the partition
     */
    public static Partition readPartition(String token, String table, String... columns) {
        List<String> columnList = Arrays.asList(columns);
        return Partition.createReadPartition(
                ByteString.copyFromUtf8(token),
                PartitionOptions.getDefaultInstance(),
                table,
                null,
                KeySet.all(),
                columnList,
                Options.fromReadOptions());
    }

    /**
     * Returns a batch transaction id.
     *
     * @param sessionId the session the batch read holds
     * @param transactionId the transaction the partitions were planned in
     * @param microseconds the read timestamp, as microseconds since the epoch
     * @return the id
     */
    public static BatchTransactionId batchTransactionId(
            String sessionId, String transactionId, long microseconds) {
        return new BatchTransactionId(
                sessionId,
                ByteString.copyFromUtf8(transactionId),
                Timestamp.ofTimeMicroseconds(microseconds));
    }

    /**
     * Returns a batch transaction id with values a test does not care about.
     *
     * @return the id
     */
    public static BatchTransactionId batchTransactionId() {
        return batchTransactionId("projects/p/instances/i/databases/d/sessions/s", "txn", 1_000L);
    }
}
