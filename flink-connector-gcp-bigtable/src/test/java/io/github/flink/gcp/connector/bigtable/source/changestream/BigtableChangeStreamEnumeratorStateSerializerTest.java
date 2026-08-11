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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamEnumeratorStateSerializerTest {

    @Test
    void roundTripsEveryCoordinatorLedger() throws IOException {
        Instant watermark = Instant.parse("2026-08-11T00:00:00Z");
        ChangeStreamPartitionSplit unassigned =
                new ChangeStreamPartitionSplit(
                        "change-stream-2",
                        ByteStringRange.unbounded().endOpen("m"),
                        Collections.emptyList(),
                        watermark);
        ChangeStreamPartitionSplit assigned =
                new ChangeStreamPartitionSplit(
                        "change-stream-3",
                        ByteStringRange.unbounded().startClosed("m"),
                        Collections.singletonList(
                                TestChangeStreamTokens.token(
                                        ByteStringRange.unbounded().startClosed("m"), "token")),
                        watermark.plusSeconds(1));
        PendingMerge merge =
                new PendingMerge(
                        ByteStringRange.unbounded(),
                        assigned.getContinuationTokens(),
                        watermark.minusSeconds(1));
        BigtableChangeStreamEnumeratorState state =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        watermark,
                        4,
                        Collections.singletonList(unassigned),
                        Collections.singletonList(assigned),
                        Collections.singletonList(merge));
        BigtableChangeStreamEnumeratorStateSerializer serializer =
                new BigtableChangeStreamEnumeratorStateSerializer();

        BigtableChangeStreamEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
    }
}
