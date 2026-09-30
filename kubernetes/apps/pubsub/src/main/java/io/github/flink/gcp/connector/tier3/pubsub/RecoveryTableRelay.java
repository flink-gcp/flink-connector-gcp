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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;

/**
 * Table entry point of the relay: the production Pub/Sub table source and sink around the same
 * observer and payload contract as the DataStream entry point.
 */
@Internal
final class RecoveryTableRelay {
    private RecoveryTableRelay() {}

    static void attach(StreamExecutionEnvironment env, RecoveryOptions options, String emulator) {
        StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        // The default format numbers ExecNodes from a static counter, which is not stable across
        // translations. Each of the two translations below instead gets its own fixed prefix, so
        // a node type repeated in both still has a distinct UID; a repeat within one translation
        // fails job graph generation with a hash collision rather than aliasing state.
        tables.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.ALWAYS);
        String connection =
                "'connector' = 'pubsub', 'project' = '"
                        + RecoveryOptions.PROJECT
                        + "', 'format' = 'raw'"
                        + (emulator == null ? "" : ", 'emulator-endpoint' = '" + emulator + "'");
        tables.executeSql(
                "CREATE TABLE pubsub_input ("
                        + "payload BYTES, "
                        + "message_id STRING METADATA FROM 'message-id' VIRTUAL, "
                        + "subscription STRING METADATA FROM 'subscription' VIRTUAL"
                        + ") WITH ("
                        + connection
                        + ", 'subscription' = '"
                        + options.input(0)
                        + ";"
                        + options.input(1)
                        + "', 'scan.parallelism' = '"
                        + options.parallelism
                        + "')");
        tables.executeSql(
                "CREATE TABLE pubsub_output (payload STRING) WITH ("
                        + connection
                        + ", 'topic' = '"
                        + options.output()
                        + "', 'sink.create-disposition' = 'create-never', 'sink.parallelism' = '"
                        + options.parallelism
                        + "')");

        uidPrefix(tables, "pubsub-table-input-v1");
        DataStream<String> observed =
                tables.toDataStream(tables.from("pubsub_input"))
                        .map(new Tagger(options))
                        .uid("pubsub-table-tagger-v1")
                        .setParallelism(options.parallelism)
                        .map(new RecoveryObserver(options))
                        .uid("pubsub-table-observer-v1")
                        .setParallelism(options.parallelism);
        uidPrefix(tables, "pubsub-table-output-v1");
        var output = tables.createStatementSet();
        output.add(tables.fromDataStream(observed).insertInto("pubsub_output"));
        output.attachAsDataStream();
    }

    private static void uidPrefix(StreamTableEnvironment tables, String prefix) {
        tables.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT,
                        prefix + "-<type>-<transformation>");
    }

    /**
     * Applies the input check both entry points share, {@link RecoveryPayload#tag(RecoveryOptions,
     * ByteString, String, String)}, to one table row.
     */
    static final class Tagger implements MapFunction<Row, String> {
        private static final long serialVersionUID = 1L;
        private final RecoveryOptions options;

        Tagger(RecoveryOptions options) {
            this.options = options;
        }

        @Override
        public String map(Row row) throws Exception {
            byte[] payload = row.<byte[]>getFieldAs("payload");
            return RecoveryPayload.tag(
                    options,
                    payload == null ? null : ByteString.copyFrom(payload),
                    row.getFieldAs("message_id"),
                    row.getFieldAs("subscription"));
        }
    }
}
