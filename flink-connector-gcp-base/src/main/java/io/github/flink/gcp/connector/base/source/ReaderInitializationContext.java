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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * Adapts a {@link SourceReaderContext} to the context a {@link DeserializationSchema} expects when
 * a source opens it.
 *
 * <p>Flink offers no adapter of its own, so every FLIP-27 source that opens a deserialization
 * schema needs one. This repository ships a base module, so the connectors share this one rather
 * than each carrying a private copy of the same two delegations.
 */
@Internal
public final class ReaderInitializationContext
        implements DeserializationSchema.InitializationContext {

    private final SourceReaderContext context;

    public ReaderInitializationContext(SourceReaderContext context) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
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
