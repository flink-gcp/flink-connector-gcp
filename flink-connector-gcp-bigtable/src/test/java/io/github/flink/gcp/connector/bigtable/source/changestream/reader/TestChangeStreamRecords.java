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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.bigtable.v2.ReadChangeStreamResponse;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.StreamContinuationToken;
import com.google.bigtable.v2.StreamPartition;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.time.Instant;

final class TestChangeStreamRecords {

    private TestChangeStreamRecords() {}

    static ChangeStreamMutation mutation(Instant commit, Instant watermark, String token) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(ByteString.copyFromUtf8("row"), "cluster", commit, 0);
        builder.deleteFamily("family");
        return (ChangeStreamMutation) builder.finishChangeStreamMutation(token, watermark);
    }

    static ChangeStreamMutation mutationWithThreeEntries(
            Instant commit, Instant watermark, String token) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(ByteString.copyFromUtf8("row"), "cluster", commit, 0);
        builder.deleteFamily("family-1");
        builder.deleteFamily("family-2");
        builder.deleteFamily("family-3");
        return (ChangeStreamMutation) builder.finishChangeStreamMutation(token, watermark);
    }

    static ChangeStreamMutation garbageCollectionMutation(
            Instant commit, Instant watermark, String token) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startGcMutation(ByteString.copyFromUtf8("row"), commit, 0);
        builder.deleteFamily("family");
        return (ChangeStreamMutation) builder.finishChangeStreamMutation(token, watermark);
    }

    static Heartbeat heartbeat(Instant watermark, String token) {
        return heartbeat(watermark, token, ByteStringRange.create("a", "z"));
    }

    static Heartbeat heartbeat(Instant watermark, String token, ByteStringRange partition) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        return (Heartbeat)
                builder.onHeartbeat(
                        ReadChangeStreamResponse.Heartbeat.newBuilder()
                                .setContinuationToken(token(partition, token))
                                .setEstimatedLowWatermark(timestamp(watermark))
                                .build());
    }

    static CloseStream close(String token) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        return (CloseStream)
                builder.onCloseStream(
                        ReadChangeStreamResponse.CloseStream.newBuilder()
                                .setStatus(
                                        com.google.rpc.Status.newBuilder()
                                                .setCode(com.google.rpc.Code.OUT_OF_RANGE_VALUE))
                                .addContinuationTokens(token("a", "m", token))
                                .addNewPartitions(partition("a", "m"))
                                .build());
    }

    static CloseStream closeWithMismatchedSuccessors() {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        return (CloseStream)
                builder.onCloseStream(
                        ReadChangeStreamResponse.CloseStream.newBuilder()
                                .setStatus(
                                        com.google.rpc.Status.newBuilder()
                                                .setCode(com.google.rpc.Code.OUT_OF_RANGE_VALUE))
                                .addContinuationTokens(token("a", "m", "left"))
                                .addContinuationTokens(token("m", "z", "right"))
                                .addNewPartitions(partition("a", "m"))
                                .build());
    }

    private static StreamContinuationToken token(String start, String end, String token) {
        return token(ByteStringRange.create(start, end), token);
    }

    private static StreamContinuationToken token(ByteStringRange partition, String token) {
        RowRange.Builder range = RowRange.newBuilder();
        if (partition.getStartBound()
                != com.google.cloud.bigtable.data.v2.models.Range.BoundType.UNBOUNDED) {
            if (partition.getStartBound()
                    == com.google.cloud.bigtable.data.v2.models.Range.BoundType.OPEN) {
                range.setStartKeyOpen(partition.getStart());
            } else {
                range.setStartKeyClosed(partition.getStart());
            }
        }
        if (partition.getEndBound()
                != com.google.cloud.bigtable.data.v2.models.Range.BoundType.UNBOUNDED) {
            if (partition.getEndBound()
                    == com.google.cloud.bigtable.data.v2.models.Range.BoundType.CLOSED) {
                range.setEndKeyClosed(partition.getEnd());
            } else {
                range.setEndKeyOpen(partition.getEnd());
            }
        }
        return StreamContinuationToken.newBuilder()
                .setPartition(StreamPartition.newBuilder().setRowRange(range))
                .setToken(token)
                .build();
    }

    private static StreamPartition partition(String start, String end) {
        return StreamPartition.newBuilder()
                .setRowRange(
                        RowRange.newBuilder()
                                .setStartKeyClosed(ByteString.copyFromUtf8(start))
                                .setEndKeyOpen(ByteString.copyFromUtf8(end)))
                .build();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
