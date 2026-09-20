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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;

/** Internal finite workload for separately admitted BigQuery checkpoint and recovery trials. */
@Internal
public final class BigQueryRecoveryJob {
    private BigQueryRecoveryJob() {}

    /** Runs one trial against tables provisioned by the external lifecycle runner. */
    public static void main(String[] args) throws Exception {
        var options = RecoveryOptions.parse(args);
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, options, sink(options, null, null));
        env.execute("BigQuery recovery " + options.runId);
    }

    static void configure(
            StreamExecutionEnvironment env, RecoveryOptions options, Sink<Long> sink) {
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        input(env, options)
                .partitionCustom(
                        (Long sequence, int partitions) ->
                                (int) (sequence / options.destinations % partitions),
                        sequence -> sequence)
                .sinkTo(sink)
                .name("BigQuery " + options.mode)
                .uid("bq1312-sink-v1")
                .setParallelism(2)
                .setMaxParallelism(128);
    }

    static SingleOutputStreamOperator<Long> input(
            StreamExecutionEnvironment env, RecoveryOptions options) {
        var source =
                new DataGeneratorSource<Long>(
                        value -> value,
                        options.records,
                        RateLimiterStrategy.perSecond(
                                (double) options.bytesPerSecond / options.mode.rowBytes),
                        Types.LONG);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuery input")
                .uid("bq1312-source-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new RecoveryInput(options))
                .name("BigQuery input identity")
                .uid("bq1312-input-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    static Sink<Long> sink(RecoveryOptions options, String grpcEndpoint, String restEndpoint) {
        var builder =
                BigQuerySink.<Long>builder()
                        .destinationResolver((sequence, context) -> options.table(sequence))
                        .serializer(new RecoveryRows(options))
                        .createDisposition(CreateDisposition.CREATE_NEVER);
        if (options.mode == RecoveryOptions.Mode.EO) {
            builder.writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                    .bufferedStreamOptions(BufferedStreamOptions.builder().build());
        } else {
            builder.writeMethod(WriteMethod.STORAGE_API_AT_LEAST_ONCE)
                    .defaultStreamOptions(DefaultStreamOptions.builder().build());
        }
        if (grpcEndpoint != null) {
            builder.emulatorEndpoint(grpcEndpoint).emulatorRestEndpoint(restEndpoint);
        }
        return ObservedSinks.observe(builder.build(), options);
    }
}
