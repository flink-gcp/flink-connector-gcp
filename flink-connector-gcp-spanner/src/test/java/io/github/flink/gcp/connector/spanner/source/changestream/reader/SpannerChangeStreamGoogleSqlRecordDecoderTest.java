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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerChangeStreamGoogleSqlRecordDecoderTest {

    private final SpannerChangeStreamGoogleSqlRecordDecoder decoder =
            new SpannerChangeStreamGoogleSqlRecordDecoder();

    @Test
    void decodesDataAndPreservesTheCompleteRecursiveTypeDescriptor() throws Exception {
        Struct column =
                Struct.newBuilder()
                        .set("name")
                        .to("tokens")
                        .set("type")
                        .to(
                                Value.json(
                                        "{\"code\":\"ARRAY\","
                                                + "\"array_element_type\":{"
                                                + "\"type_annotation\":\"SEARCH\","
                                                + "\"code\":\"TOKENLIST\"},"
                                                + "\"type_annotation\":\"SEARCH\"}"))
                        .set("is_primary_key")
                        .to(false)
                        .set("ordinal_position")
                        .to(2L)
                        .build();
        Struct mod =
                Struct.newBuilder()
                        .set("keys")
                        .to(Value.json("{\"id\":1}"))
                        .set("new_values")
                        .to(Value.json("{\"name\":\"Ada\"}"))
                        .set("old_values")
                        .to(Value.json(null))
                        .build();
        Struct data =
                Struct.newBuilder()
                        .set("commit_timestamp")
                        .to(timestamp("2026-08-12T01:02:03.123456789Z"))
                        .set("record_sequence")
                        .to("0007")
                        .set("server_transaction_id")
                        .to("tx-1")
                        .set("is_last_record_in_transaction_in_partition")
                        .to(true)
                        .set("table_name")
                        .to("documents")
                        .set("column_types")
                        .to(Value.structArray(column.getType(), Collections.singletonList(column)))
                        .set("mods")
                        .to(Value.structArray(mod.getType(), Collections.singletonList(mod)))
                        .set("mod_type")
                        .to("UPDATE")
                        .set("value_capture_type")
                        .to("NEW_ROW_AND_OLD_VALUES")
                        .set("number_of_records_in_transaction")
                        .to(3L)
                        .set("number_of_partitions_in_transaction")
                        .to(2L)
                        .set("transaction_tag")
                        .to("app=test")
                        .set("is_system_transaction")
                        .to(false)
                        .build();

        SpannerChangeStreamRecord.Data decoded =
                (SpannerChangeStreamRecord.Data) decoder.decode(dataRow(data));
        DataChangeRecord record = decoded.record;

        assertThat(record.getCommitTimestamp())
                .isEqualTo(Instant.parse("2026-08-12T01:02:03.123456789Z"));
        assertThat(record.getRecordSequence()).isEqualTo("0007");
        assertThat(record.getTableName()).isEqualTo("documents");
        assertThat(record.getColumnTypes())
                .singleElement()
                .satisfies(
                        columnType -> {
                            assertThat(columnType.getTypeCode()).contains("ARRAY");
                            assertThat(columnType.getTypeDescriptorJson())
                                    .isEqualTo(
                                            "{\"array_element_type\":{\"code\":\"TOKENLIST\","
                                                    + "\"type_annotation\":\"SEARCH\"},"
                                                    + "\"code\":\"ARRAY\","
                                                    + "\"type_annotation\":\"SEARCH\"}");
                        });
        assertThat(record.getMods())
                .singleElement()
                .satisfies(
                        decodedMod -> {
                            assertThat(decodedMod.getKeysJson()).isEqualTo("{\"id\":1}");
                            assertThat(decodedMod.getNewValuesJson())
                                    .contains("{\"name\":\"Ada\"}");
                            assertThat(decodedMod.getOldValuesJson()).isEmpty();
                        });
    }

    @Test
    void decodesHeartbeatAndInitialPlusNamedParents() throws Exception {
        Struct heartbeat =
                Struct.newBuilder().set("timestamp").to(timestamp("2026-01-01T00:00:01Z")).build();
        SpannerChangeStreamRecord.Heartbeat decodedHeartbeat =
                (SpannerChangeStreamRecord.Heartbeat) decoder.decode(heartbeatRow(heartbeat));
        assertThat(decodedHeartbeat.position()).isEqualTo(Instant.parse("2026-01-01T00:00:01Z"));

        Struct child =
                Struct.newBuilder()
                        .set("token")
                        .to("child")
                        .set("parent_partition_tokens")
                        .toStringArray(Arrays.asList(null, "parent"))
                        .build();
        Struct children =
                Struct.newBuilder()
                        .set("start_timestamp")
                        .to(timestamp("2026-01-01T00:00:02Z"))
                        .set("record_sequence")
                        .to("2")
                        .set("child_partitions")
                        .to(Value.structArray(child.getType(), Collections.singletonList(child)))
                        .build();
        SpannerChangeStreamRecord.Children decodedChildren =
                (SpannerChangeStreamRecord.Children) decoder.decode(childrenRow(children));

        assertThat(decodedChildren.children)
                .singleElement()
                .satisfies(
                        decodedChild -> {
                            assertThat(decodedChild.initialParent).isTrue();
                            assertThat(decodedChild.parentTokens).containsExactly("parent");
                        });
    }

    private static Struct dataRow(Struct data) {
        return row(envelope(data, null, null));
    }

    private static Struct heartbeatRow(Struct heartbeat) {
        return row(envelope(null, heartbeat, null));
    }

    private static Struct childrenRow(Struct children) {
        return row(envelope(null, null, children));
    }

    private static Struct envelope(Struct data, Struct heartbeat, Struct children) {
        Type dataType = data == null ? emptyData().getType() : data.getType();
        Type heartbeatType = heartbeat == null ? emptyHeartbeat().getType() : heartbeat.getType();
        Type childrenType = children == null ? emptyChildren().getType() : children.getType();
        return Struct.newBuilder()
                .set("data_change_record")
                .to(Value.structArray(dataType, nullableList(data)))
                .set("heartbeat_record")
                .to(Value.structArray(heartbeatType, nullableList(heartbeat)))
                .set("child_partitions_record")
                .to(Value.structArray(childrenType, nullableList(children)))
                .build();
    }

    private static Struct row(Struct envelope) {
        return Struct.newBuilder()
                .set("ChangeRecord")
                .to(Value.structArray(envelope.getType(), Collections.singletonList(envelope)))
                .build();
    }

    private static java.util.List<Struct> nullableList(Struct value) {
        return value == null ? Collections.emptyList() : Collections.singletonList(value);
    }

    private static Struct emptyData() {
        return Struct.newBuilder().set("placeholder").to(0L).build();
    }

    private static Struct emptyHeartbeat() {
        return Struct.newBuilder().set("timestamp").to(timestamp("1970-01-01T00:00:00Z")).build();
    }

    private static Struct emptyChildren() {
        return Struct.newBuilder().set("placeholder").to(0L).build();
    }

    private static Timestamp timestamp(String value) {
        Instant instant = Instant.parse(value);
        return Timestamp.ofTimeSecondsAndNanos(instant.getEpochSecond(), instant.getNano());
    }
}
