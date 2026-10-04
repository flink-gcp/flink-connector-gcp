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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Set;

/**
 * Forwards the Flink 2.x {@link Source#declareWatermarks()} operation, absent with its return type
 * on Flink 1.20. The {@code flink.compat} property selects this or the {@code java-flink1} twin
 * (ADR-0054, ADR-0160).
 */
@Internal
interface CrossVersionSource<T, S extends SourceSplit, E> extends Source<T, S, E> {

    /**
     * Returns the wrapped source used by the version-specific delegation.
     *
     * @return the wrapped source
     */
    Source<T, S, E> delegate();

    @Override
    default Set<? extends WatermarkDeclaration> declareWatermarks() {
        return delegate().declareWatermarks();
    }
}
