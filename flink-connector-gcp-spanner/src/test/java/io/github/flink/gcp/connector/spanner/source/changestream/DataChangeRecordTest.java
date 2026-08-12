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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataChangeRecordTest {

    @Test
    void genericFlinkSerializerRoundTripsCollectionFields() throws Exception {
        DataChangeRecord record = record();
        TypeSerializer<DataChangeRecord> serializer =
                TypeInformation.of(DataChangeRecord.class)
                        .createSerializer(new SerializerConfigImpl());

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(record, output);
        DataChangeRecord restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id");
        assertThat(restored.getMods()).extracting(Mod::getKeysJson).containsExactly("{\"id\":1}");
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

    private static DataChangeRecord record() {
        return new DataChangeRecord(
                Instant.parse("2026-08-12T00:00:00Z"),
                "1",
                "tx",
                true,
                "singers",
                Collections.singletonList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1)),
                Collections.singletonList(new Mod("{\"id\":1}", "{}", null)),
                ModType.UPDATE,
                ValueCaptureType.NEW_VALUES,
                1,
                1,
                "",
                false);
    }
}
