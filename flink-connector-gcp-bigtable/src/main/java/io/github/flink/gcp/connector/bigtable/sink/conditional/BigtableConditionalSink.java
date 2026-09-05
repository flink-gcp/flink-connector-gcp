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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestSinks;

import java.io.IOException;

/**
 * Atomic per-row conditional writes with at-least-once delivery. Checkpoints drain accepted
 * requests; successful results are discarded. A replay can select a different branch. Requires
 * single-cluster app-profile routing with single-row transactions enabled.
 *
 * @param <T> the input type
 */
@PublicEvolving
public final class BigtableConditionalSink<T> implements CrossVersionSink<T> {
    private static final long serialVersionUID = 1L;
    private final SingleRowRequestConfig<T> config;

    BigtableConditionalSink(
            ConditionalConfig<T> config, FailureHandler<? super FailedRequest> handler) {
        this.config =
                new SingleRowRequestConfig<>(
                        config.destinationResolver,
                        config.sinkSerializer(),
                        config.appProfileId,
                        config.requestOptions,
                        handler,
                        config.serviceAccountKeyFile,
                        config.emulatorEndpoint);
    }

    /**
     * Creates a builder requiring a destination and serialization schema.
     *
     * @param <T> the input type
     * @return the builder
     */
    public static <T> BigtableConditionalSinkBuilder<T> builder() {
        return new BigtableConditionalSinkBuilder<>();
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return SingleRowRequestSinks.createWriter(config, context);
    }
}
