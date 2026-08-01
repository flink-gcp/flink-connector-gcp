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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * The Flink 1.20 variant of the cross-version seam (selected by {@code -Dflink.compat=flink1};
 * see the {@code src/main/java-flink2} twin for the full story): Flink 1.20 still declares the
 * deprecated {@code createWriter(Sink.InitContext)} abstract, so a sink implementing only the
 * {@code WriterInitContext} overload does not compile there — while Flink 2.x removed {@code
 * Sink.InitContext} outright, so this default cannot live in shared source.
 *
 * <p>The bridge is compile-only: Flink 1.20's runtime creates writers through {@code
 * createWriter(WriterInitContext)} (its default delegates new-to-old for legacy sinks, never the
 * reverse) — measured, the whole test suite runs green on 1.20 with this bridge throwing.
 */
@Internal
public interface CrossVersionSink<InputT> extends Sink<InputT> {

    @Override
    @SuppressWarnings("deprecation")
    default SinkWriter<InputT> createWriter(Sink.InitContext context) {
        throw new UnsupportedOperationException(
                "Flink 1.20's runtime creates writers through createWriter(WriterInitContext);"
                        + " this bridge exists only to satisfy the 1.20 compiler");
    }
}
