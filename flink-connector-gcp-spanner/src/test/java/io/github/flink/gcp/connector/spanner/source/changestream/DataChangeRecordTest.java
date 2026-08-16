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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataChangeRecordTest {

    @Test
    void genericFlinkSerializerRoundTripsCollectionFields() throws Exception {
        DataChangeRecord record = record();
        TypeSerializer<DataChangeRecord> serializer = serializer();

        assertThat(serializer).isInstanceOf(DataChangeRecordSerializer.class);
        assertThat(serializer.copy(record)).isSameAs(record);

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(record, output);
        DataChangeRecord restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id", "name");
        assertThat(restored.getColumnTypes()).containsExactlyElementsOf(record.getColumnTypes());
        assertThat(restored.getMods()).containsExactlyElementsOf(record.getMods());
        assertThat(restored.getCommitTimestamp()).isEqualTo(record.getCommitTimestamp());
        assertThat(restored.getRecordSequence()).isEqualTo("00000001");
        assertThat(restored.getServerTransactionId()).isEqualTo("tx-1");
        assertThat(restored.isLastRecordInTransactionInPartition()).isFalse();
        assertThat(restored.getTableName()).isEqualTo("singers");
        assertThat(restored.getModType()).isEqualTo(ModType.UPDATE);
        assertThat(restored.getValueCaptureType())
                .isEqualTo(ValueCaptureType.NEW_ROW_AND_OLD_VALUES);
        assertThat(restored.getNumberOfRecordsInTransaction()).isEqualTo(2);
        assertThat(restored.getNumberOfPartitionsInTransaction()).isEqualTo(3);
        assertThat(restored.getTransactionTag()).isEqualTo("import");
        assertThat(restored.isSystemTransaction()).isTrue();
    }

    @Test
    void collectionViewsRemainUnmodifiable() {
        DataChangeRecord record = record();

        assertThatThrownBy(() -> record.getColumnTypes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> record.getMods().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void projectedRecordRoundTripsWithoutRestoringFilteredColumns() throws Exception {
        DataChangeRecord source =
                new DataChangeRecord(
                        Instant.parse("2026-08-12T00:00:00Z"),
                        "1",
                        "tx",
                        true,
                        "singers",
                        Arrays.asList(
                                new DataChangeRecord.ColumnType(
                                        "id", "{\"code\":\"INT64\"}", true, 1),
                                new DataChangeRecord.ColumnType(
                                        "secret", "{\"code\":\"FUTURE_TYPE\"}", false, 2)),
                        Collections.singletonList(
                                new Mod("{\"id\":1}", "{\"secret\":\"hidden\"}", null)),
                        ModType.UPDATE,
                        ValueCaptureType.NEW_VALUES,
                        1,
                        1,
                        "",
                        false);
        SpannerChangeStreamRecordFilter filter =
                new SpannerChangeStreamRecordFilter(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(Pattern.compile("singers\\.secret")),
                        false);
        DataChangeRecord projected = filter.filter(source).getRecord();
        TypeSerializer<DataChangeRecord> serializer =
                TypeInformation.of(DataChangeRecord.class)
                        .createSerializer(new SerializerConfigImpl());

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(projected, output);
        DataChangeRecord restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id");
        assertThat(restored.getMods().get(0).getKeysJson()).isEqualTo("{\"id\":1}");
        assertThat(restored.getMods().get(0).getNewValuesJson()).contains("{}");
    }

    @Test
    void binaryCopyRetainsTheCompleteRecord() throws Exception {
        TypeSerializer<DataChangeRecord> serializer = serializer();
        String value = "日本語".repeat(5_000);
        DataChangeRecord expected =
                copyWithMods(
                        record(),
                        Collections.singletonList(
                                new Mod("{\"id\":1}", "{\"value\":\"" + value + "\"}", null)));
        DataOutputSerializer serialized = new DataOutputSerializer(256);
        serializer.serialize(expected, serialized);
        DataOutputSerializer copied = new DataOutputSerializer(256);

        serializer.copy(new DataInputDeserializer(serialized.getCopyOfBuffer()), copied);
        DataChangeRecord restored =
                serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));

        assertRecordEquals(restored, expected);
    }

    @Test
    void roundTripsMultibyteJsonLargerThanModifiedUtfLimit() throws Exception {
        String value = "日本語".repeat(25_000);
        DataChangeRecord large =
                copyWithMods(
                        record(),
                        Collections.singletonList(
                                new Mod("{\"id\":1}", "{\"value\":\"" + value + "\"}", null)));

        DataOutputSerializer output = new DataOutputSerializer(70_256);
        serializer().serialize(large, output);
        DataChangeRecord restored =
                serializer().deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getMods().get(0).getNewValuesJson())
                .contains("{\"value\":\"" + value + "\"}");
    }

    @Test
    void rejectsANegativeStringLength() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(16);
        output.writeLong(0L);
        output.writeInt(0);
        output.writeInt(-1);

        assertThatThrownBy(
                        () ->
                                serializer()
                                        .deserialize(
                                                new DataInputDeserializer(
                                                        output.getCopyOfBuffer())))
                .isInstanceOf(IOException.class)
                .hasMessage("Negative string length: -1");
    }

    @Test
    void rejectsNegativeCollectionCounts() throws Exception {
        DataOutputSerializer negativeColumnCount = prefixThroughTableName();
        negativeColumnCount.writeInt(-1);

        assertThatThrownBy(() -> deserialize(negativeColumnCount))
                .isInstanceOf(IOException.class)
                .hasMessage("Negative column type count: -1");

        DataOutputSerializer negativeModCount = prefixThroughTableName();
        negativeModCount.writeInt(0);
        negativeModCount.writeInt(-1);

        assertThatThrownBy(() -> deserialize(negativeModCount))
                .isInstanceOf(IOException.class)
                .hasMessage("Negative mod count: -1");
    }

    @Test
    void rejectsAnUnknownModType() throws Exception {
        DataOutputSerializer output = prefixThroughTableName();
        output.writeInt(0);
        output.writeInt(0);
        writeString("REPLACE", output);

        assertThatThrownBy(() -> deserialize(output))
                .isInstanceOf(IOException.class)
                .hasMessage("Unknown ModType: REPLACE");
    }

    private static TypeSerializer<DataChangeRecord> serializer() {
        return TypeInformation.of(DataChangeRecord.class)
                .createSerializer(new SerializerConfigImpl());
    }

    private static DataChangeRecord deserialize(DataOutputSerializer output) throws IOException {
        return serializer().deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }

    private static DataOutputSerializer prefixThroughTableName() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeLong(0L);
        output.writeInt(0);
        writeString("1", output);
        writeString("tx", output);
        output.writeBoolean(false);
        writeString("singers", output);
        return output;
    }

    private static void writeString(String value, DataOutputSerializer output) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void assertRecordEquals(DataChangeRecord actual, DataChangeRecord expected) {
        assertThat(actual.getCommitTimestamp()).isEqualTo(expected.getCommitTimestamp());
        assertThat(actual.getRecordSequence()).isEqualTo(expected.getRecordSequence());
        assertThat(actual.getServerTransactionId()).isEqualTo(expected.getServerTransactionId());
        assertThat(actual.isLastRecordInTransactionInPartition())
                .isEqualTo(expected.isLastRecordInTransactionInPartition());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getColumnTypes()).containsExactlyElementsOf(expected.getColumnTypes());
        assertThat(actual.getMods()).containsExactlyElementsOf(expected.getMods());
        assertThat(actual.getModType()).isEqualTo(expected.getModType());
        assertThat(actual.getValueCaptureType()).isEqualTo(expected.getValueCaptureType());
        assertThat(actual.getNumberOfRecordsInTransaction())
                .isEqualTo(expected.getNumberOfRecordsInTransaction());
        assertThat(actual.getNumberOfPartitionsInTransaction())
                .isEqualTo(expected.getNumberOfPartitionsInTransaction());
        assertThat(actual.getTransactionTag()).isEqualTo(expected.getTransactionTag());
        assertThat(actual.isSystemTransaction()).isEqualTo(expected.isSystemTransaction());
    }

    private static DataChangeRecord copyWithMods(DataChangeRecord record, List<Mod> mods) {
        return new DataChangeRecord(
                record.getCommitTimestamp(),
                record.getRecordSequence(),
                record.getServerTransactionId(),
                record.isLastRecordInTransactionInPartition(),
                record.getTableName(),
                record.getColumnTypes(),
                mods,
                record.getModType(),
                record.getValueCaptureType(),
                record.getNumberOfRecordsInTransaction(),
                record.getNumberOfPartitionsInTransaction(),
                record.getTransactionTag(),
                record.isSystemTransaction());
    }

    private static DataChangeRecord record() {
        return new DataChangeRecord(
                Instant.parse("2026-08-12T00:00:00.123456789Z"),
                "00000001",
                "tx-1",
                false,
                "singers",
                Arrays.asList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1),
                        new DataChangeRecord.ColumnType("name", "{\"code\":\"STRING\"}", false, 2)),
                Arrays.asList(
                        new Mod("{\"id\":1}", "{\"name\":\"Ada\"}", "null"),
                        new Mod("{\"id\":2}", null, "{\"name\":\"Grace\"}")),
                ModType.UPDATE,
                ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
                2,
                3,
                "import",
                true);
    }
}
