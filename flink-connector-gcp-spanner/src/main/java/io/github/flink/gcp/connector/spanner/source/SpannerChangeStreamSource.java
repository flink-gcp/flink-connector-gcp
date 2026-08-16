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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamEnumeratorStateSerializer;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplitSerializer;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.SpannerChangeStreamReader;

/** FLIP-27 source for Cloud Spanner Change Streams. */
@Public
public final class SpannerChangeStreamSource<T>
        implements Source<T, SpannerChangeStreamPartitionSplit, SpannerChangeStreamEnumeratorState>,
                ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final SpannerChangeStreamSourceConfig<T> config;

    SpannerChangeStreamSource(SpannerChangeStreamSourceConfig<T> config) {
        this.config = config;
    }

    public static <T> SpannerChangeStreamSourceBuilder<T> builder() {
        return new SpannerChangeStreamSourceBuilder<>();
    }

    @VisibleForTesting
    SpannerChangeStreamSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.endTimestamp == null ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, SpannerChangeStreamPartitionSplit> createReader(
            SourceReaderContext context) throws Exception {
        config.deserializer.open(new ReaderInitializationContext(context));
        return new SpannerChangeStreamReader<>(context, config);
    }

    @Override
    public SplitEnumerator<SpannerChangeStreamPartitionSplit, SpannerChangeStreamEnumeratorState>
            createEnumerator(SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context) {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<SpannerChangeStreamPartitionSplit, SpannerChangeStreamEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context,
                    SpannerChangeStreamEnumeratorState checkpoint) {
        return enumerator(context, checkpoint);
    }

    private SpannerChangeStreamSplitEnumerator enumerator(
            SplitEnumeratorContext<SpannerChangeStreamPartitionSplit> context,
            SpannerChangeStreamEnumeratorState restored) {
        return new SpannerChangeStreamSplitEnumerator(
                context,
                config.coordinatorClientFactory,
                config.startPosition,
                java.util.Optional.ofNullable(config.resumeFallback),
                config.endTimestamp,
                config.heartbeatMillis,
                restored);
    }

    @Override
    public SimpleVersionedSerializer<SpannerChangeStreamPartitionSplit> getSplitSerializer() {
        return new SpannerChangeStreamPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SpannerChangeStreamEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new SpannerChangeStreamEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.deserializer.getProducedType();
    }

    private static final class ReaderInitializationContext
            implements DeserializationSchema.InitializationContext {

        private final SourceReaderContext context;

        private ReaderInitializationContext(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return context.metricGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return context.getUserCodeClassLoader();
        }
    }
}
