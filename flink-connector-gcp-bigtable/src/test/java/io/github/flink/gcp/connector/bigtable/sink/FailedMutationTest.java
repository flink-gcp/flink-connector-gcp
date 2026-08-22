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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailedMutation}. */
class FailedMutationTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void carriesTheWholeMutationAsItsPayload() throws Exception {
        RowMutationEntry entry =
                RowMutationEntry.create("row-1").setCell("cf", "q", 1_000L, "value");

        FailedMutation failed = FailedMutation.of(TABLE, entry, "rejected", null);

        assertThat(failed.getConnector()).isEqualTo("bigtable");
        assertThat(failed.describeDestination()).isEqualTo("p.i.orders");
        assertThat(failed.getEntry()).isSameAs(entry);
        assertThat(failed.getRowKey()).isEqualTo(ByteString.copyFromUtf8("row-1"));
        assertThat(failed.getErrorMessage()).isEqualTo("rejected");
        assertThat(failed.getCause()).isNull();

        // The point of carrying the proto rather than the row key alone: a dead-letter consumer
        // recovers every mutation of the row, not just which row it was.
        MutateRowsRequest.Entry recovered =
                MutateRowsRequest.Entry.parseFrom(failed.getPayloadBytes());
        assertThat(recovered.getRowKey()).isEqualTo(ByteString.copyFromUtf8("row-1"));
        assertThat(recovered.getMutationsList()).hasSize(1);
        assertThat(recovered.getMutations(0).getSetCell().getFamilyName()).isEqualTo("cf");
        assertThat(recovered.getMutations(0).getSetCell().getTimestampMicros()).isEqualTo(1_000L);
    }

    @Test
    void carriesNoPayloadWhenSerializationItselfFailed() {
        Exception cause = new IllegalStateException("boom");

        FailedMutation failed = FailedMutation.of(TABLE, null, "not serializable", cause);

        assertThat(failed.getEntry()).isNull();
        assertThat(failed.getRowKey()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.getCause()).isSameAs(cause);
        assertThat(failed.getDestination()).isEqualTo(TABLE);
        assertThat(failed)
                .hasToString(
                        "FailedMutation{destination=p.i.orders, mutation=null,"
                                + " errorMessage=not serializable}");
    }

    @Test
    void rendersThePayloadSizeAndKeepsTheRowKeyOutOfTheLine() {
        // The shape the siblings share: FailedTask, FailedRow and FailedMessage all print the size
        // of the failed payload, and Spanner's FailedMutation carries no key at all. It is the
        // whole mutation that is measured, not the row key -- a key's length is nearly constant
        // and answers nothing, where the mutation's size is what an operator asks after an
        // INVALID_ARGUMENT on a batch. A handler that wants the row calls getRowKey().
        FailedMutation failed = failureFor(ByteString.copyFromUtf8("row-1"));
        int payload = failed.getPayloadBytes().size();

        assertThat(failed)
                .hasToString(
                        "FailedMutation{destination=p.i.orders, mutation="
                                + payload
                                + " bytes, errorMessage=rejected}");
        assertThat(payload).isGreaterThan("row-1".length());
        assertThat(failed.toString()).doesNotContain("row-1");
        assertThat(failed.getRowKey()).isEqualTo(ByteString.copyFromUtf8("row-1"));
    }

    @Test
    void keepsABinaryRowKeyOutOfTheLineToo() {
        // The case toStringUtf8() served worst: it decoded invalid UTF-8 to U+FFFD rather than
        // failing, so 0xFE and 0xFF arrived as one character that identified neither, while a key
        // that was valid UTF-8 went into the line exactly.
        String rendered = failureFor(ByteString.copyFrom(new byte[] {(byte) 0xFE})).toString();

        assertThat(rendered).doesNotContain("�").contains("mutation=").contains(" bytes");
    }

    @Test
    void requiresADestinationAndAMessage() {
        assertThatThrownBy(() -> FailedMutation.of(null, null, "m", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FailedMutation.of(TABLE, null, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static FailedMutation failureFor(ByteString rowKey) {
        return FailedMutation.of(
                TABLE,
                RowMutationEntry.create(rowKey).setCell("cf", "q", 1_000L, "value"),
                "rejected",
                null);
    }
}
