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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.StreamContinuationToken;
import com.google.bigtable.v2.StreamPartition;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.InvalidProtocolBufferException;

/** Builds tokens for ranges the SDK's public test factory cannot represent when unbounded. */
public final class TestChangeStreamTokens {

    private TestChangeStreamTokens() {}

    public static ChangeStreamContinuationToken token(ByteStringRange partition, String value) {
        RowRange.Builder range = RowRange.newBuilder();
        if (partition.getStartBound() != BoundType.UNBOUNDED) {
            range.setStartKeyClosed(partition.getStart());
        }
        if (partition.getEndBound() != BoundType.UNBOUNDED) {
            range.setEndKeyOpen(partition.getEnd());
        }
        StreamContinuationToken proto =
                StreamContinuationToken.newBuilder()
                        .setPartition(StreamPartition.newBuilder().setRowRange(range))
                        .setToken(value)
                        .build();
        try {
            return ChangeStreamContinuationToken.fromByteString(proto.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }
}
