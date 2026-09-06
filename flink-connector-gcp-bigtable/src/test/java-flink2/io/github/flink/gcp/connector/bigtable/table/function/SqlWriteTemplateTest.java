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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlWriteTemplateTest {
    static Configuration config(String... pairs) {
        Configuration config = new Configuration();
        config.setString("table.exec.async-scalar.max-attempts", "1");
        setting(config, "project", "project");
        setting(config, "instance", "instance");
        setting(config, "table", "table");
        for (int i = 0; i < pairs.length; i += 2) {
            setting(config, pairs[i], pairs[i + 1]);
        }
        return config;
    }

    static void setting(Configuration config, String key, String value) {
        config.setString("bigtable.functions.test." + key, value);
    }

    static SqlWriteTemplate compile(Configuration config, boolean conditional, DataType... values) {
        List<DataType> types = new ArrayList<>(List.of(DataTypes.STRING(), DataTypes.STRING()));
        types.addAll(List.of(values));
        return SqlWriteTemplate.compile("test", conditional, config, types);
    }

    static Configuration increment() {
        return config(
                "rules.0.operation",
                "increment",
                "rules.0.family",
                "cf",
                "rules.0.qualifier",
                "count",
                "rules.0.value-argument",
                "0");
    }

    static GenericRowData input(Object... values) {
        Object[] fields = new Object[values.length + 1];
        fields[0] = StringData.fromString("key");
        System.arraycopy(values, 0, fields, 1, values.length);
        return GenericRowData.of(fields);
    }

    @Test
    void serializedTemplateRetainsItsConfigurationAndCopiesInputBytes() throws Exception {
        Configuration config =
                config(
                        "rules.0.operation",
                        "append",
                        "rules.0.family",
                        "cf",
                        "rules.0.qualifier-base64",
                        "AP8=",
                        "rules.0.value-argument",
                        "0");
        SqlWriteTemplate template =
                InstantiationUtil.clone(compile(config, false, DataTypes.BYTES()));
        setting(config, "table", "changed");
        byte[] value = {0, -1};
        var request = template.request(input(value));
        value[0] = 42;
        FakeClients client = new FakeClients();
        request.start(client, template.destination);
        var proto =
                client.rmw.get(0).toProto(RequestContext.create("project", "instance", "profile"));
        assertThat(proto.getTableName()).endsWith("/tables/table");
        assertThat(proto.getRules(0).getColumnQualifier())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, -1}));
        assertThat(proto.getRules(0).getAppendValue())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, -1}));
    }

    @Test
    void nestedFiltersAndEveryMutationPreserveTheirWireOrderAndTypedValues() throws Exception {
        Configuration config =
                config(
                        "predicate.type",
                        "chain",
                        "predicate.children.0.type",
                        "family-equals",
                        "predicate.children.0.family",
                        "cf",
                        "predicate.children.1.type",
                        "interleave",
                        "predicate.children.1.children.0.type",
                        "qualifier-equals",
                        "predicate.children.1.children.0.qualifier",
                        "a",
                        "predicate.children.1.children.1.type",
                        "value-equals",
                        "predicate.children.1.children.1.value-argument",
                        "0",
                        "then.0.operation",
                        "set-cell",
                        "then.0.family",
                        "cf",
                        "then.0.qualifier",
                        "set",
                        "then.0.value-argument",
                        "0",
                        "then.0.timestamp-micros",
                        "-1",
                        "then.1.operation",
                        "delete-cells",
                        "then.1.family",
                        "cf",
                        "then.1.qualifier",
                        "old",
                        "then.1.start-timestamp-micros",
                        "0",
                        "then.1.end-timestamp-argument",
                        "1",
                        "then.2.operation",
                        "delete-family",
                        "then.2.family",
                        "obsolete",
                        "then.3.operation",
                        "delete-row",
                        "then.4.operation",
                        "add-to-cell",
                        "then.4.family",
                        "sum",
                        "then.4.qualifier",
                        "",
                        "then.4.value-argument",
                        "1",
                        "then.4.timestamp-micros",
                        "0",
                        "then.5.operation",
                        "merge-to-cell",
                        "then.5.family",
                        "state",
                        "then.5.qualifier",
                        "",
                        "then.5.value-base64",
                        "AP8=",
                        "then.5.timestamp-micros",
                        "1000");
        SqlWriteTemplate template = compile(config, true, DataTypes.STRING(), DataTypes.BIGINT());
        FakeClients client = new FakeClients();
        template.request(input(StringData.fromString("v"), 2000L))
                .start(client, template.destination);
        var proto =
                client.conditional
                        .get(0)
                        .toProto(RequestContext.create("project", "instance", "profile"));
        var filters = proto.getPredicateFilter().getChain().getFiltersList();
        assertThat(filters).hasSize(2);
        assertThat(filters.get(0).getFamilyNameRegexFilter()).isEqualTo("cf");
        assertThat(filters.get(1).getInterleave().getFiltersList()).hasSize(2);
        assertThat(proto.getTrueMutationsList()).hasSize(6);
        assertThat(proto.getTrueMutations(0).getSetCell().getTimestampMicros()).isEqualTo(-1);
        assertThat(
                        proto.getTrueMutations(1)
                                .getDeleteFromColumn()
                                .getTimeRange()
                                .getEndTimestampMicros())
                .isEqualTo(2000);
        assertThat(proto.getTrueMutations(2).hasDeleteFromFamily()).isTrue();
        assertThat(proto.getTrueMutations(3).hasDeleteFromRow()).isTrue();
        assertThat(proto.getTrueMutations(4).getAddToCell().getInput().getIntValue())
                .isEqualTo(2000);
        assertThat(proto.getTrueMutations(5).getMergeToCell().getInput().getBytesValue())
                .isEqualTo(ByteString.copyFrom(new byte[] {0, -1}));
        assertThat(proto.getFalseMutationsList()).isEmpty();
    }

    @Test
    void timestampAndCellLimitFiltersKeepTheirConfiguredBounds() throws Exception {
        Configuration config =
                config(
                        "predicate.type",
                        "chain",
                        "predicate.children.0.type",
                        "timestamp-range",
                        "predicate.children.0.start-timestamp-micros",
                        "0",
                        "predicate.children.0.end-timestamp-argument",
                        "0",
                        "predicate.children.1.type",
                        "cells-per-column",
                        "predicate.children.1.count",
                        "2",
                        "predicate.children.2.type",
                        "cells-per-row",
                        "predicate.children.2.count",
                        "3",
                        "then.0.operation",
                        "delete-row");
        SqlWriteTemplate template = compile(config, true, DataTypes.BIGINT());
        FakeClients client = new FakeClients();
        template.request(input(2000L)).start(client, template.destination);
        var filters =
                client.conditional
                        .get(0)
                        .toProto(RequestContext.create("project", "instance", "profile"))
                        .getPredicateFilter()
                        .getChain()
                        .getFiltersList();
        assertThat(filters.get(0).getTimestampRangeFilter().getStartTimestampMicros()).isZero();
        assertThat(filters.get(0).getTimestampRangeFilter().getEndTimestampMicros())
                .isEqualTo(2000);
        assertThat(filters.get(1).getCellsPerColumnLimitFilter()).isEqualTo(2);
        assertThat(filters.get(2).getCellsPerRowLimitFilter()).isEqualTo(3);
    }

    @Test
    void numericOutputFollowsTheLastRuleForTheCellRatherThanResponseOrderOrByteLength() {
        Configuration config = increment();
        setting(config, "rules.1.operation", "append");
        setting(config, "rules.1.family", "cf");
        setting(config, "rules.1.qualifier", "count");
        setting(config, "rules.1.value-utf8", "x");
        setting(config, "rules.2.operation", "increment");
        setting(config, "rules.2.family", "other");
        setting(config, "rules.2.qualifier", "count");
        setting(config, "rules.2.value-int64", "-1");
        SqlWriteTemplate template = compile(config, false, DataTypes.BIGINT());
        ByteString number = ByteString.copyFrom(ByteBuffer.allocate(8).putLong(-1).array());
        Row changed =
                (Row)
                        template.result(
                                new BigtableRow(
                                        ByteString.copyFromUtf8("key"),
                                        List.of(
                                                new BigtableRow.Cell(
                                                        "other",
                                                        ByteString.copyFromUtf8("count"),
                                                        2000,
                                                        number,
                                                        List.of()),
                                                new BigtableRow.Cell(
                                                        "cf",
                                                        ByteString.copyFromUtf8("count"),
                                                        1000,
                                                        number,
                                                        List.of()))));
        Row[] cells = (Row[]) changed.getField(1);
        assertThat(cells[0].getField(4)).isEqualTo(-1L);
        assertThat(cells[1].getField(4)).isNull();
        assertThat((byte[]) cells[1].getField(2)).hasSize(8);
    }

    @ParameterizedTest
    @CsvSource({
        "rules.0.value-argument,1",
        "rules.0.value-argument,-1",
        "rules.0.value-argument,01",
        "rules.2.operation,increment",
        "rules.0.unknown,x",
        "request-timeout,0 ms",
        "request-timeout,3 min",
        "project,p/i"
    })
    void invalidConfigurationNamesTheSuppliedKey(String key, String value) {
        Configuration config = increment();
        setting(config, key, value);
        assertThatThrownBy(() -> compile(config, false, DataTypes.BIGINT()))
                .hasMessageContaining("bigtable.functions.test." + key);
    }

    @ParameterizedTest
    @ValueSource(strings = {"AA", "AB==", "_w=="})
    void malformedBase64QualifiersReachTheCanonicalEncodingCheck(String value) {
        Configuration config = increment();
        config.removeKey("bigtable.functions.test.rules.0.qualifier");
        setting(config, "rules.0.qualifier-base64", value);
        assertThatThrownBy(() -> compile(config, false, DataTypes.BIGINT()))
                .hasMessage(
                        "Option 'bigtable.functions.test.rules.0.qualifier-base64' must be canonical padded Base64.");
    }

    @Test
    void emptyAppendOperandsNameTheBindingBeforeAnyRpc() {
        Configuration config =
                config(
                        "rules.0.operation",
                        "append",
                        "rules.0.family",
                        "cf",
                        "rules.0.qualifier",
                        "q",
                        "rules.0.value-argument",
                        "0");
        for (boolean binary : List.of(false, true)) {
            SqlWriteTemplate template =
                    compile(config, false, binary ? DataTypes.BYTES() : DataTypes.STRING());
            Object operand = binary ? new byte[0] : StringData.fromString("");
            assertThatThrownBy(() -> template.request(input(operand)))
                    .hasMessageContaining("bigtable.functions.test.rules.0.value-argument")
                    .hasMessageContaining("must not be empty");
        }
    }

    @Test
    void aReferencedNullInTheUnselectedBranchStillFailsBeforeAnyRpc() {
        Configuration config =
                config(
                        "predicate.type",
                        "row-exists",
                        "then.0.operation",
                        "delete-row",
                        "otherwise.0.operation",
                        "set-cell",
                        "otherwise.0.family",
                        "cf",
                        "otherwise.0.qualifier",
                        "q",
                        "otherwise.0.value-argument",
                        "0");
        SqlWriteTemplate template = compile(config, true, DataTypes.STRING());
        assertThatThrownBy(() -> template.request(input((Object) null)))
                .hasMessageContaining("otherwise.0.value-argument")
                .hasMessageContaining("NULL");
    }

    @Test
    void invalidRuntimeTimestampFailsUnderItsArgumentKey() {
        Configuration config =
                config(
                        "predicate.type",
                        "row-exists",
                        "then.0.operation",
                        "set-cell",
                        "then.0.family",
                        "cf",
                        "then.0.qualifier",
                        "q",
                        "then.0.value-utf8",
                        "v",
                        "then.0.timestamp-argument",
                        "0");
        SqlWriteTemplate template = compile(config, true, DataTypes.BIGINT());
        assertThatThrownBy(() -> template.request(input(-2L)))
                .hasMessageContaining("then.0.timestamp-argument");
    }

    static final class FakeClients implements SingleRowClientFactory, SingleRowClient {
        private static final long serialVersionUID = 1L;
        final List<ConditionalRowMutation> conditional = new ArrayList<>();
        final List<ReadModifyWriteRow> rmw = new ArrayList<>();
        final List<SettableApiFuture<Boolean>> conditions = new ArrayList<>();
        final List<SettableApiFuture<com.google.cloud.bigtable.data.v2.models.Row>> rows =
                new ArrayList<>();
        int opens;
        int releases;
        int closes;

        @Override
        public SingleRowClient create(TableDestination destination) {
            opens++;
            return this;
        }

        @Override
        public void release(TableDestination destination) {
            releases++;
        }

        @Override
        public void close() {
            closes++;
        }

        @Override
        public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation request) {
            conditional.add(request);
            var answer = SettableApiFuture.<Boolean>create();
            conditions.add(answer);
            return answer;
        }

        @Override
        public ApiFuture<com.google.cloud.bigtable.data.v2.models.Row> readModifyWriteRow(
                ReadModifyWriteRow request) {
            rmw.add(request);
            var answer = SettableApiFuture.<com.google.cloud.bigtable.data.v2.models.Row>create();
            rows.add(answer);
            return answer;
        }
    }
}
