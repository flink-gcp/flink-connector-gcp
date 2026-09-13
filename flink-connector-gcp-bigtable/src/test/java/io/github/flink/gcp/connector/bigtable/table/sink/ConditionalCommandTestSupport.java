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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.testutils.TestContexts;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Drives the real factory and request serializer with a recording transport. */
final class ConditionalCommandTestSupport {
    static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("k", DataTypes.STRING()),
                    Column.physical("expected", DataTypes.BYTES()),
                    Column.physical("value", DataTypes.BYTES()),
                    Column.physical("delta", DataTypes.BIGINT()),
                    Column.physical("ts", DataTypes.BIGINT()),
                    Column.physical("end", DataTypes.BIGINT()),
                    Column.physical("unused", DataTypes.ARRAY(DataTypes.STRING())));
    static final RowType ROW_TYPE = (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();
    static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    private ConditionalCommandTestSupport() {}

    static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigtable");
        options.put("project", "p");
        options.put("instance", "i");
        options.put("table", "t");
        // Production writer creation must never fall through to ADC.
        options.put("emulator-endpoint", "localhost:1");
        options.put("sink.write-mode", "conditional");
        options.put("sink.conditional.row-key-column", "k");
        options.put("sink.conditional.predicate", "row-exists");
        options.put("sink.conditional.then.0.operation", "delete-row");
        return options;
    }

    static GenericRowData row() {
        return GenericRowData.of(
                StringData.fromString("key"),
                new byte[] {0, 10, (byte) 255},
                new byte[] {1, 10, (byte) 254},
                -5L,
                1000L,
                2000L,
                null);
    }

    static void cell(Map<String, String> options, String branch, int index, String operation) {
        String prefix = "sink.conditional." + branch + "." + index + ".";
        options.put(prefix + "operation", operation);
        options.put(prefix + "family", "cf");
        options.put(prefix + "qualifier", "q");
    }

    @SuppressWarnings("unchecked")
    static SingleRowRequestConfig<RowData> config(Map<String, String> options) throws Exception {
        DynamicTableSink dynamic = FactoryMocks.createTableSink(SCHEMA, options);
        Sink<?> sink =
                ((SinkV2Provider)
                                dynamic.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        Field field = sink.getClass().getDeclaredField("config");
        field.setAccessible(true);
        return (SingleRowRequestConfig<RowData>) field.get(sink);
    }

    static CheckAndMutateRowRequest wire(Map<String, String> options, RowData input)
            throws Exception {
        FakeClients clients = new FakeClients();
        config(options).getSerializer().serialize(input, TestContexts.NO_OP).start(clients, TABLE);
        return clients.sent.get(0);
    }

    static final class FakeClients implements SingleRowClientFactory, SingleRowClient {
        final List<CheckAndMutateRowRequest> sent = new ArrayList<>();
        final List<SettableApiFuture<Boolean>> answers = new ArrayList<>();
        int opened;
        int released;
        int closes;

        @Override
        public SingleRowClient create(TableDestination destination) {
            opened++;
            return this;
        }

        @Override
        public void release(TableDestination destination) {
            released++;
        }

        @Override
        public void close() {
            closes++;
        }

        @Override
        public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation request) {
            sent.add(request.toProto(RequestContext.create("p", "i", "profile")));
            SettableApiFuture<Boolean> answer = SettableApiFuture.create();
            answers.add(answer);
            return answer;
        }

        @Override
        public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow request) {
            throw new AssertionError("Conditional commands must use CheckAndMutateRow");
        }
    }
}
