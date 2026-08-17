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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * A {@link TableCreateOptionsProvider} returning one fixed set of creation options for every
 * destination.
 *
 * <p>A named class (instead of a synthesized lambda) so that nothing the connector itself puts in
 * the job graph depends on {@code SerializedLambda} synthetic-method identity, which is a compiler
 * detail rather than a versioned API — the same reason {@link FixedDestinationResolver} is one.
 */
@Internal
final class FixedTableCreateOptionsProvider implements TableCreateOptionsProvider {

    private static final long serialVersionUID = 1L;

    private final TableCreateOptions options;

    /**
     * Creates a provider returning the given options for every destination.
     *
     * @param options the creation options
     */
    public FixedTableCreateOptionsProvider(TableCreateOptions options) {
        this.options = Preconditions.checkNotNull(options, "options must not be null");
    }

    @Override
    public TableCreateOptions optionsFor(TableDestination destination) {
        return options;
    }
}
