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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.BigtableLineage;
import io.github.flink.gcp.connector.bigtable.sink.CrossVersionSink;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowRequestSinks;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Atomic per-row conditional writes with at-least-once delivery. Checkpoints drain accepted
 * requests; successful results are discarded. A replay can select a different branch. Requires
 * single-cluster app-profile routing with single-row transactions enabled.
 *
 * <p>The builder-returned sink implements {@code LineageVertexProvider}. Its effective fixed
 * destination reports namespace {@code bigtable://{project}/{instance}}, name {@code {table}} and a
 * {@code gcp} physical-resource facet. Dynamic destinations produce an empty dataset list, without
 * resolver evaluation. Extraction calls no user schema and opens no client. The supported Flink 2.x
 * versions extract the metadata automatically; Flink 1.20 supports direct inspection only.
 *
 * @param <T> the input type
 */
@PublicEvolving
public final class BigtableConditionalSink<T>
        implements CrossVersionSink<T>, LineageVertexProvider {
    private static final long serialVersionUID = 1L;
    private final SingleRowRequestConfig<T> config;
    @Nullable private final String lineageTableName;

    BigtableConditionalSink(
            ConditionalConfig<T> config, FailureHandler<? super FailedRequest> handler) {
        this(
                new SingleRowRequestConfig<>(
                        config.destinationResolver,
                        config.sinkSerializer(),
                        config.appProfileId,
                        config.requestOptions,
                        handler,
                        config.serviceAccountKeyFile,
                        config.emulatorEndpoint),
                null);
    }

    private BigtableConditionalSink(
            SingleRowRequestConfig<T> config, @Nullable String lineageTableName) {
        this.config = config;
        this.lineageTableName = lineageTableName;
    }

    /** Returns a copy carrying the logical Table identity with the same request configuration. */
    @Internal
    public BigtableConditionalSink<T> withTableLineage(String logicalName) {
        return new BigtableConditionalSink<>(
                config, Objects.requireNonNull(logicalName, "logicalName"));
    }

    @Override
    public LineageVertex getLineageVertex() {
        return BigtableLineage.sink(config.getDestinationResolver(), lineageTableName);
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
