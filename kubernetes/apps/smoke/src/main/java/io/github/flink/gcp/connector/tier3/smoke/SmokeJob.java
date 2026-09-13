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

package io.github.flink.gcp.connector.tier3.smoke;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

/** Internal application whose keyed sequence and lineage survive a Flink restore. */
@Internal
public final class SmokeJob {

    private SmokeJob() {}

    /** Runs the smoke job with storage and checkpoint settings supplied by the deployment. */
    public static void main(String[] args) throws Exception {
        SmokeOptions options = SmokeOptions.parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        progress(env, options).sinkTo(new PrintSink<>()).uid("smoke-output-v1").setParallelism(1);
        env.execute("Tier-3 generic smoke");
    }

    static DataStream<String> progress(StreamExecutionEnvironment env, SmokeOptions options) {
        // Datagen is bounded even for a long run; streaming mode keeps checkpoints enabled.
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        return env.fromSource(
                        new DataGeneratorSource<>(
                                index -> index,
                                options.records,
                                RateLimiterStrategy.perSecond(options.recordsPerSecond),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "Smoke sequence")
                .uid("smoke-sequence-v1")
                .setParallelism(1)
                .setMaxParallelism(1)
                .keyBy(value -> (int) (value % SmokeVerifier.KEYS))
                .process(new SmokeVerifier(options))
                .name("Smoke sequence verifier")
                .uid("smoke-verifier-v1")
                .setParallelism(1)
                .setMaxParallelism(1);
    }
}
