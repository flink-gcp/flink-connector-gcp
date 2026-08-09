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

package io.github.flink.gcp.connector.bigtable.source.readrows;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowRangeSplitSerializer}. */
@Timeout(30)
class RowRangeSplitSerializerTest {

    private final RowRangeSplitSerializer serializer = new RowRangeSplitSerializer();

    private static ByteString bytes(int... values) {
        byte[] raw = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            raw[i] = (byte) values[i];
        }
        return ByteString.copyFrom(raw);
    }

    static Stream<ByteStringRange> everyBoundCombination() {
        return Stream.of(
                ByteStringRange.unbounded(),
                ByteStringRange.unbounded().startClosed("a"),
                ByteStringRange.unbounded().startOpen("a"),
                ByteStringRange.unbounded().endClosed("z"),
                ByteStringRange.unbounded().endOpen("z"),
                ByteStringRange.unbounded().startClosed("a").endClosed("z"),
                ByteStringRange.unbounded().startClosed("a").endOpen("z"),
                ByteStringRange.unbounded().startOpen("a").endClosed("z"),
                ByteStringRange.unbounded().startOpen("a").endOpen("z"));
    }

    @ParameterizedTest
    @MethodSource("everyBoundCombination")
    void roundTripsEveryBoundCombination(ByteStringRange range) throws IOException {
        RowRangeSplit split = new RowRangeSplit("0", range);

        RowRangeSplit back =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(back).isEqualTo(split);
        assertThat(back.getRange().getStartBound()).isEqualTo(range.getStartBound());
        assertThat(back.getRange().getEndBound()).isEqualTo(range.getEndBound());
    }

    @Test
    void roundTripsARowKeyHoldingANulByte() {
        // A row key is arbitrary bytes; writeUTF's modified UTF-8 encodes 0x00 as two bytes and
        // would hand back a key that is not the one that was checkpointed.
        ByteString key = bytes('a', 0x00, 'b');

        assertThat(roundTrip(ByteStringRange.unbounded().startClosed(key)).getRange().getStart())
                .isEqualTo(key);
    }

    @Test
    void roundTripsARowKeyThatIsNotValidUtf8() {
        ByteString key = bytes(0xFF, 0xFE, 0x80);

        assertThat(roundTrip(ByteStringRange.unbounded().endOpen(key)).getRange().getEnd())
                .isEqualTo(key);
    }

    @Test
    void roundTripsAFullLengthRowKey() {
        // Bigtable's row keys go up to 4 KB, which is past writeUTF's 65535-byte ceiling only in
        // aggregate — but well past the point where a length prefix stops being optional.
        byte[] raw = new byte[4096];
        for (int i = 0; i < raw.length; i++) {
            raw[i] = (byte) i;
        }
        ByteString key = ByteString.copyFrom(raw);

        assertThat(roundTrip(ByteStringRange.unbounded().startOpen(key)).getRange().getStart())
                .isEqualTo(key);
    }

    @Test
    void roundTripsAnEmptyTruncatedRange() {
        RowRangeSplit split =
                new RowRangeSplit("0", ByteStringRange.unbounded().startOpen("z").endClosed("z"));

        assertThat(roundTrip(split.getRange())).isEqualTo(split);
    }

    @Test
    void rejectsAVersionItDidNotWrite() throws IOException {
        byte[] serialized =
                serializer.serialize(new RowRangeSplit("0", ByteStringRange.unbounded()));

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Bigtable row range split serialization version")
                .hasMessageContaining(String.valueOf(serializer.getVersion()));
    }

    @Test
    void rejectsAnUnknownBoundCode() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("0");
        out.writeByte(9);
        out.writeInt(1);
        out.write(new byte[] {'a'});

        assertThatThrownBy(
                        () ->
                                RowRangeSplitSerializer.readSplit(
                                        new DataInputDeserializer(out.getCopyOfBuffer())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unknown start bound code 9");
    }

    @Test
    void rejectsANegativeKeyLength() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF("0");
        out.writeByte(1);
        out.writeInt(-1);

        assertThatThrownBy(
                        () ->
                                RowRangeSplitSerializer.readSplit(
                                        new DataInputDeserializer(out.getCopyOfBuffer())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative start key length");
    }

    @Test
    void writesAndReadsBackWithoutAVersionTagForTheEnumeratorState() throws IOException {
        RowRangeSplit split =
                new RowRangeSplit("3", ByteStringRange.unbounded().startOpen("m").endClosed("z"));
        DataOutputSerializer out = new DataOutputSerializer(64);

        RowRangeSplitSerializer.writeSplit(out, split);
        // A trailing field proves the helper leaves the stream positioned for the caller's own
        // data, which is what lets the enumerator state embed a list of splits.
        out.writeInt(4242);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());

        assertThat(RowRangeSplitSerializer.readSplit(in)).isEqualTo(split);
        assertThat(in.readInt()).isEqualTo(4242);
    }

    private RowRangeSplit roundTrip(ByteStringRange range) {
        try {
            RowRangeSplit split = new RowRangeSplit("0", range);
            return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
