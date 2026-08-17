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
                DataChangeRecord.builder()
                        .commitTimestamp(Instant.parse("2026-08-12T00:00:00Z"))
                        .recordSequence("1")
                        .serverTransactionId("tx")
                        .lastRecordInTransactionInPartition(true)
                        .tableName("singers")
                        .columnTypes(
                                Arrays.asList(
                                        new DataChangeRecord.ColumnType(
                                                "id", "{\"code\":\"INT64\"}", true, 1),
                                        new DataChangeRecord.ColumnType(
                                                "secret", "{\"code\":\"FUTURE_TYPE\"}", false, 2)))
                        .mods(
                                Collections.singletonList(
                                        new Mod("{\"id\":1}", "{\"secret\":\"hidden\"}", null)))
                        .modType(ModType.UPDATE)
                        .valueCaptureType(ValueCaptureType.NEW_VALUES)
                        .numberOfRecordsInTransaction(1)
                        .numberOfPartitionsInTransaction(1)
                        .transactionTag("")
                        .systemTransaction(false)
                        .build();
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

    @Test
    void buildNamesEveryValueThatWasNotSupplied() {
        assertThatThrownBy(() -> DataChangeRecord.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContainingAll(
                        "commitTimestamp",
                        "recordSequence",
                        "serverTransactionId",
                        "tableName",
                        "columnTypes",
                        "mods",
                        "modType",
                        "valueCaptureType",
                        "transactionTag",
                        "lastRecordInTransactionInPartition",
                        "systemTransaction",
                        "numberOfRecordsInTransaction",
                        "numberOfPartitionsInTransaction");
    }

    @Test
    void aPrimitiveLeftUnsetIsNotSilentlyDefaulted() {
        // The point of tracking set-ness: `false` and `0` are values Spanner reports, so a builder
        // that defaulted them would hand back a record claiming a transaction produced no records.
        // Zero explicitly supplied is accepted; zero never supplied is refused.
        DataChangeRecord zeroCount = record().toBuilder().numberOfRecordsInTransaction(0).build();

        assertThat(zeroCount.getNumberOfRecordsInTransaction()).isZero();
        assertThatThrownBy(
                        () ->
                                DataChangeRecord.builder()
                                        .commitTimestamp(Instant.EPOCH)
                                        .recordSequence("1")
                                        .serverTransactionId("tx")
                                        .lastRecordInTransactionInPartition(false)
                                        .tableName("singers")
                                        .columnTypes(Collections.emptyList())
                                        .mods(Collections.emptyList())
                                        .modType(ModType.INSERT)
                                        .valueCaptureType(ValueCaptureType.NEW_VALUES)
                                        .numberOfPartitionsInTransaction(1)
                                        .transactionTag("")
                                        .systemTransaction(false)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("numberOfRecordsInTransaction")
                .hasMessageNotContainingAny("numberOfPartitionsInTransaction", "modType");
    }

    @Test
    void toBuilderCarriesEveryValue() {
        DataChangeRecord record = record();

        assertThat(record.toBuilder().build()).isEqualTo(record).hasSameHashCodeAs(record);
    }

    @Test
    void equalityCoversEveryValue() {
        DataChangeRecord record = record();
        List<DataChangeRecord> mutated =
                Arrays.asList(
                        record.toBuilder().commitTimestamp(Instant.EPOCH).build(),
                        record.toBuilder().recordSequence("00000002").build(),
                        record.toBuilder().serverTransactionId("tx-2").build(),
                        record.toBuilder().lastRecordInTransactionInPartition(true).build(),
                        record.toBuilder().tableName("albums").build(),
                        record.toBuilder().columnTypes(Collections.emptyList()).build(),
                        record.toBuilder().mods(Collections.emptyList()).build(),
                        record.toBuilder().modType(ModType.DELETE).build(),
                        record.toBuilder().valueCaptureType(ValueCaptureType.NEW_VALUES).build(),
                        record.toBuilder().numberOfRecordsInTransaction(99).build(),
                        record.toBuilder().numberOfPartitionsInTransaction(99).build(),
                        record.toBuilder().transactionTag("export").build(),
                        record.toBuilder().systemTransaction(false).build());

        // One case per field: an equals that dropped any one of the thirteen would let its
        // mutation compare equal here.
        assertThat(mutated).hasSize(13).doesNotContain(record);
        assertThat(record).isEqualTo(record).isNotEqualTo(null).isNotEqualTo("not a record");
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
        return record.toBuilder().mods(mods).build();
    }

    private static DataChangeRecord record() {
        return DataChangeRecord.builder()
                .commitTimestamp(Instant.parse("2026-08-12T00:00:00.123456789Z"))
                .recordSequence("00000001")
                .serverTransactionId("tx-1")
                .lastRecordInTransactionInPartition(false)
                .tableName("singers")
                .columnTypes(
                        Arrays.asList(
                                new DataChangeRecord.ColumnType(
                                        "id", "{\"code\":\"INT64\"}", true, 1),
                                new DataChangeRecord.ColumnType(
                                        "name", "{\"code\":\"STRING\"}", false, 2)))
                .mods(
                        Arrays.asList(
                                new Mod("{\"id\":1}", "{\"name\":\"Ada\"}", "null"),
                                new Mod("{\"id\":2}", null, "{\"name\":\"Grace\"}")))
                .modType(ModType.UPDATE)
                .valueCaptureType(ValueCaptureType.NEW_ROW_AND_OLD_VALUES)
                .numberOfRecordsInTransaction(2)
                .numberOfPartitionsInTransaction(3)
                .transactionTag("import")
                .systemTransaction(true)
                .build();
    }
}
