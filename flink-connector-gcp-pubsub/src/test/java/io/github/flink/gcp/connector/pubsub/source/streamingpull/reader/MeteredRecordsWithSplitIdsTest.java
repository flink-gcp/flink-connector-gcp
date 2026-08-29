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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the retention boundary between subscriber buffers and Flink's fetcher batches. */
class MeteredRecordsWithSplitIdsTest {

    @Test
    void countsEachRecordUntilTheSourceReaderTakesIt() {
        TestReaderMetrics readerMetrics = new TestReaderMetrics();
        PubsubMessage first = message("first", "x");
        PubsubMessage second = message("second", "xxxxxxxx");
        RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
        builder.add("split-a", first);
        builder.add("split-a", second);
        MeteredRecordsWithSplitIds records =
                new MeteredRecordsWithSplitIds(
                        builder.build(),
                        2,
                        first.getSerializedSize() + second.getSerializedSize(),
                        readerMetrics.metrics());

        assertThat(readerMetrics.gauge("fetcherBufferedMessages")).isEqualTo(2);
        assertThat(readerMetrics.gauge("fetcherBufferedBytes"))
                .isEqualTo(first.getSerializedSize() + second.getSerializedSize());

        assertThat(records.nextSplit()).isEqualTo("split-a");
        assertThat(records.nextRecordFromSplit()).isEqualTo(first);

        assertThat(readerMetrics.gauge("fetcherBufferedMessages")).isEqualTo(1);
        assertThat(readerMetrics.gauge("fetcherBufferedBytes"))
                .isEqualTo(second.getSerializedSize());

        records.recycle();
        records.recycle();

        assertThat(readerMetrics.gauge("fetcherBufferedMessages")).isZero();
        assertThat(readerMetrics.gauge("fetcherBufferedBytes")).isZero();
    }

    private static PubsubMessage message(String id, String payload) {
        return PubsubMessage.newBuilder()
                .setMessageId(id)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
    }
}
