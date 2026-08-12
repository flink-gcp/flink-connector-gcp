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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamPostgreSqlRecordDecoderTest {

    private final SpannerChangeStreamPostgreSqlRecordDecoder decoder =
            new SpannerChangeStreamPostgreSqlRecordDecoder();

    @Test
    void decodesEveryDataFieldAndPreservesSpecialAndUnknownTypes() throws Exception {
        String json =
                "{\"data_change_record\":{"
                        + "\"commit_timestamp\":\"2026-08-12T01:02:03.123456789Z\","
                        + "\"record_sequence\":\"0007\","
                        + "\"server_transaction_id\":\"tx-1\","
                        + "\"is_last_record_in_transaction_in_partition\":true,"
                        + "\"table_name\":\"documents\","
                        + "\"column_types\":["
                        + "{\"name\":\"tokens\",\"type\":{\"code\":\"TOKENLIST\","
                        + "\"type_annotation\":\"SEARCH\"},\"is_primary_key\":false,"
                        + "\"ordinal_position\":2},"
                        + "{\"name\":\"future\",\"type\":{\"code\":\"FUTURE_TYPE\","
                        + "\"array_element_type\":{\"code\":\"STRING\"}},"
                        + "\"is_primary_key\":true,\"ordinal_position\":1}],"
                        + "\"mods\":[{\"keys\":{\"id\":\"a\"},"
                        + "\"new_values\":{\"name\":\"Ada\"},"
                        + "\"old_values\":{\"name\":\"Augusta\"}}],"
                        + "\"mod_type\":\"UPDATE\","
                        + "\"value_capture_type\":\"NEW_ROW_AND_OLD_VALUES\","
                        + "\"number_of_records_in_transaction\":3,"
                        + "\"number_of_partitions_in_transaction\":2,"
                        + "\"transaction_tag\":\"app=test\","
                        + "\"is_system_transaction\":false}}";

        SpannerChangeStreamRecord.Data decoded =
                (SpannerChangeStreamRecord.Data) decoder.decode(pgJson(json));
        DataChangeRecord record = decoded.record;

        assertThat(record.getCommitTimestamp())
                .isEqualTo(Instant.parse("2026-08-12T01:02:03.123456789Z"));
        assertThat(record.getRecordSequence()).isEqualTo("0007");
        assertThat(record.getServerTransactionId()).isEqualTo("tx-1");
        assertThat(record.isLastRecordInTransactionInPartition()).isTrue();
        assertThat(record.getTableName()).isEqualTo("documents");
        assertThat(record.getModType()).isEqualTo(ModType.UPDATE);
        assertThat(record.getValueCaptureType()).isEqualTo(ValueCaptureType.NEW_ROW_AND_OLD_VALUES);
        assertThat(record.getNumberOfRecordsInTransaction()).isEqualTo(3);
        assertThat(record.getNumberOfPartitionsInTransaction()).isEqualTo(2);
        assertThat(record.getTransactionTag()).isEqualTo("app=test");
        assertThat(record.isSystemTransaction()).isFalse();

        assertThat(record.getColumnTypes()).hasSize(2);
        assertThat(record.getColumnTypes().get(0).getTypeCode()).contains("TOKENLIST");
        assertThat(record.getColumnTypes().get(0).getTypeDescriptorJson())
                .isEqualTo("{\"code\":\"TOKENLIST\",\"type_annotation\":\"SEARCH\"}");
        assertThat(record.getColumnTypes().get(1).getTypeCode()).contains("FUTURE_TYPE");
        assertThat(record.getColumnTypes().get(1).getTypeDescriptorJson())
                .isEqualTo(
                        "{\"array_element_type\":{\"code\":\"STRING\"},"
                                + "\"code\":\"FUTURE_TYPE\"}");

        assertThat(record.getMods())
                .singleElement()
                .satisfies(
                        mod -> {
                            assertThat(mod.getKeysJson()).isEqualTo("{\"id\":\"a\"}");
                            assertThat(mod.getNewValuesJson()).contains("{\"name\":\"Ada\"}");
                            assertThat(mod.getOldValuesJson()).contains("{\"name\":\"Augusta\"}");
                        });
    }

    @Test
    void rejectsAnEnvelopeWithMoreThanOneRecordKind() {
        assertThatThrownBy(
                        () ->
                                decoder.decode(
                                        pgJson(
                                                "{\"heartbeat_record\":{\"timestamp\":"
                                                        + "\"2026-01-01T00:00:01Z\"},"
                                                        + "\"child_partitions_record\":{}}")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("exactly one record member");
    }

    @Test
    void recordModelRoundTripDoesNotLoseDescriptorOrAbsentNullDistinction() throws Exception {
        SpannerChangeStreamRecord.Data decoded =
                (SpannerChangeStreamRecord.Data)
                        decoder.decode(
                                pgJson(
                                        "{\"data_change_record\":{"
                                                + "\"commit_timestamp\":\"2026-01-01T00:00:00Z\","
                                                + "\"record_sequence\":\"1\","
                                                + "\"server_transaction_id\":\"tx\","
                                                + "\"is_last_record_in_transaction_in_partition\":true,"
                                                + "\"table_name\":\"t\","
                                                + "\"column_types\":[{\"name\":\"a\","
                                                + "\"type\":{\"code\":\"ARRAY\","
                                                + "\"array_element_type\":{\"code\":\"TOKENLIST\"}},"
                                                + "\"is_primary_key\":false,\"ordinal_position\":1}],"
                                                + "\"mods\":[{\"keys\":{},\"old_values\":null}],"
                                                + "\"mod_type\":\"INSERT\","
                                                + "\"value_capture_type\":\"NEW_VALUES\","
                                                + "\"number_of_records_in_transaction\":1,"
                                                + "\"number_of_partitions_in_transaction\":1,"
                                                + "\"transaction_tag\":\"\","
                                                + "\"is_system_transaction\":false}}"));

        DataChangeRecord copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(decoded.record),
                        getClass().getClassLoader());

        assertThat(copy.getColumnTypes().get(0).getTypeDescriptorJson())
                .isEqualTo(
                        "{\"array_element_type\":{\"code\":\"TOKENLIST\"},"
                                + "\"code\":\"ARRAY\"}");
        assertThat(copy.getMods().get(0).getNewValuesJson()).isEmpty();
        assertThat(copy.getMods().get(0).getOldValuesJson()).contains("null");
    }

    @Test
    void decodesHeartbeatAndChildPartitions() throws Exception {
        SpannerChangeStreamRecord.Heartbeat heartbeat =
                (SpannerChangeStreamRecord.Heartbeat)
                        decoder.decode(
                                pgJson(
                                        "{\"heartbeat_record\":{\"timestamp\":"
                                                + "\"2026-01-01T00:00:01Z\"}}"));
        assertThat(heartbeat.position()).isEqualTo(Instant.parse("2026-01-01T00:00:01Z"));

        SpannerChangeStreamRecord.Children children =
                (SpannerChangeStreamRecord.Children)
                        decoder.decode(
                                pgJson(
                                        "{\"child_partitions_record\":{"
                                                + "\"start_timestamp\":\"2026-01-01T00:00:02Z\","
                                                + "\"record_sequence\":\"2\","
                                                + "\"child_partitions\":[{\"token\":\"child\","
                                                + "\"parent_partition_tokens\":[null,\"parent\"]}]}}"));

        assertThat(children.startTimestamp).isEqualTo(Instant.parse("2026-01-01T00:00:02Z"));
        assertThat(children.children)
                .singleElement()
                .satisfies(
                        child -> {
                            assertThat(child.token).isEqualTo("child");
                            assertThat(child.initialParent).isTrue();
                            assertThat(child.parentTokens).containsExactly("parent");
                        });
    }

    private static Struct pgJson(String json) {
        return Struct.newBuilder().set("ChangeRecord").to(Value.pgJsonb(json)).build();
    }
}
